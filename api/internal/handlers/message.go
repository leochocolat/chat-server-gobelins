package handlers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"chat-gobelin-api/internal"
	"chat-gobelin-api/internal/users"
	"chat-gobelin-api/internal/channels"
	"chat-gobelin-api/internal/messages"
	"chat-gobelin-api/internal/websockets"
)

type messageHandler struct {
	hub *websockets.BroadcastHub
	messageRepo messages.Repo
}

func NewMessageHandler(h *websockets.BroadcastHub, s messages.Repo) (*messageHandler, error) {
	return &messageHandler{
		hub: h,
		messageRepo: s,
	}, nil
}

type createMessageRequest struct {
	Content string `json:"content" binding:"required"`
	Channel int64 `json:"channel" binding:"required"`
	Metadata *json.RawMessage `json:"metadata"`
	Anonymous bool `json:"anonymous"`
	PostedAt *time.Time `json:"posted_at"`
}

type editMessageRequest struct {
	Content string `json:"content" binding:"required"`
	ID int64 `json:"id" binding:"required"`
	Metadata *json.RawMessage `json:"metadata"`
}

func (h messageHandler) GetMessages(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()

	channelIDString := request.URL.Query().Get("channel")
	if channelIDString == "" {
		log.Printf("No channel found: %s", request.URL.RawQuery)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	channelID, err := strconv.ParseInt(channelIDString, 10, 64)
	if err != nil {
		log.Printf("Error converting channel ID: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	channel := channels.Channel{
		ID: channelID,
	}

	messages, err := h.messageRepo.GetByChannel(ctx, channel)
	if err != nil {
		log.Printf("Error getting messages: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	namespace := internal.GetNamespace(ctx)
	for i := range messages {
		messages[i].Metadata = internal.FilterMetadata(messages[i].Metadata, namespace)
		messages[i].Anonymize()
	}

	result, err := json.Marshal(messages)
	if err != nil {
		log.Printf("Error marshaling value: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(result)
}

func (h messageHandler) CreateMessage(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()

	userRaw := ctx.Value("user")
	user := userRaw.(*users.User)
	if user == nil {
		log.Printf("No user found")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	decoder := json.NewDecoder(request.Body)

	var req createMessageRequest
	err := decoder.Decode(&req)

	if err != nil {
		log.Printf("Error decoding request: %s", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	//TODO: validation
	channel := channels.Channel{
		ID: req.Channel,
	}

	meta := json.RawMessage("{}")
	namespacedMeta := make(map[string]json.RawMessage)
	if req.Metadata != nil {
		namespace := internal.GetNamespace(ctx)
		if namespace == "" {
			writeMissingNamespace(writer)
			return
		}
		namespacedMeta[namespace] = *req.Metadata
		meta, err = json.Marshal(namespacedMeta)
		if err != nil {
			log.Printf("Error namespacing metadatas: %s", err.Error())
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	message, err := h.messageRepo.Create(ctx, req.Content, meta, channel, *user, req.Anonymous, req.PostedAt)
	if err != nil {
		log.Printf("Error creating message: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	message.Metadata = namespacedMeta
	message.Anonymize()

	result, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error marshaling value: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	//Scheduled messages reach the sockets when they're posted, as they reach GET /channel/message
	if message.IsPosted() {
		h.hub.Broadcast(websockets.MessageCreated, result, []int64{channel.ID})
	} else {
		h.scheduleBroadcast(*message, internal.GetNamespace(ctx))
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(result)
}

func (h messageHandler) EditMessage(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()

	userRaw := ctx.Value("user")
	user := userRaw.(*users.User)
	if user == nil {
		log.Printf("No user found")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	decoder := json.NewDecoder(request.Body)

	var req editMessageRequest
	err := decoder.Decode(&req)
	if err != nil {
		log.Printf("Error decoding request: %s", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	message, err := h.messageRepo.GetByID(ctx, req.ID)
	if err != nil {
		log.Printf("Error retrieving message: %s", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	var meta json.RawMessage
	namespacedMeta := message.Metadata
	if req.Metadata != nil {
		namespace := internal.GetNamespace(ctx)
		if namespace == "" {
			writeMissingNamespace(writer)
			return
		}
		namespacedMeta[namespace] = *req.Metadata
		meta, err = json.Marshal(namespacedMeta)
		if err != nil {
			log.Printf("Error namespacing metadatas: %s", err.Error())
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	message, err = h.messageRepo.Edit(ctx, *message, req.Content, meta)
	if err != nil {
		log.Printf("Error editing message: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	message.Metadata = internal.FilterMetadata(message.Metadata, internal.GetNamespace(ctx))
	message.Anonymize()

	result, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error marshaling value: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	//Clients get scheduled messages when they're posted, with their latest edits
	if message.IsPosted() {
		h.hub.Broadcast(websockets.MessageEdited, result, []int64{message.ChannelID})
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(result)
}

//The broadcast is lost if the server restarts before, the message is still returned by GET /channel/message
func (h messageHandler) scheduleBroadcast(message messages.Message, namespace string) {
	postedAt, err := time.Parse(time.RFC3339Nano, message.PostedAt)
	if err != nil {
		log.Printf("Error parsing posted_at of message %d: %s", message.ID, err.Error())
		return
	}

	time.AfterFunc(time.Until(postedAt), func() {
		//Read again to send the latest edits
		posted, err := h.messageRepo.GetByID(context.Background(), message.ID)
		if err != nil {
			log.Printf("Error retrieving scheduled message: %s", err.Error())
			return
		}
		posted.Metadata = internal.FilterMetadata(posted.Metadata, namespace)
		posted.Anonymize()

		result, err := json.Marshal(posted)
		if err != nil {
			log.Printf("Error marshaling value: %s", err.Error())
			return
		}
		h.hub.Broadcast(websockets.MessageCreated, result, []int64{posted.ChannelID})
	})
}
