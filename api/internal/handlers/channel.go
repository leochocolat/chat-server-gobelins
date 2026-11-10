package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"chat-gobelin-api/internal"
	"chat-gobelin-api/internal/users"
	"chat-gobelin-api/internal/channels"
	"chat-gobelin-api/internal/websockets"
)

type channelHandler struct {
	hub *websockets.BroadcastHub
	channelRepo channels.Repo
	userRepo users.Repo
}

func NewChannelHandler(h *websockets.BroadcastHub, s channels.Repo, u users.Repo) (*channelHandler, error) {
	return &channelHandler{
		hub: h,
		channelRepo: s,
		userRepo: u,
	}, nil
}

type createChannelRequest struct {
	Name string `json:"name" binding:"required"`
	Metadata *json.RawMessage `json:"metadata"`
}

type editChannelRequest struct {
	ID int64 `json:"id" binding:"required"`
	Name string `json:"name" binding:"required"`
	Metadata *json.RawMessage `json:"metadata"`
}

type EnterChannelRequest struct {
	ChannelName string `json:"channel" binding:"required"`
}

//Sent to the channel members when someone joins
type channelJoinedEvent struct {
	Channel channels.Channel `json:"channel"`
	User    websockets.User  `json:"user"`
}

func (h channelHandler) Create(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()

	userRaw := ctx.Value("user")
	user := userRaw.(*users.User)
	if user == nil {
		log.Printf("No user found")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	decoder := json.NewDecoder(request.Body)

	var req createChannelRequest
	err := decoder.Decode(&req)

	if err != nil {
		log.Printf("Error decoding request: %s", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
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

	channel, err := h.channelRepo.Create(ctx, req.Name, meta)
	if err != nil {
		log.Printf("Error creating channel: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	channel.Metadata = namespacedMeta

	err = h.channelRepo.AddUserToChannel(ctx, *channel, *user)
	if err != nil {
		log.Printf("Error adding user to new channel: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	result, err := json.Marshal(channel)
	if err != nil {
		log.Printf("Error marshaling value: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
	writer.Write(result)
}

func (h channelHandler) EnterChannel(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()

	userRaw := ctx.Value("user")
	user := userRaw.(*users.User)
	if user == nil {
		log.Printf("No user found")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	decoder := json.NewDecoder(request.Body)

	var req EnterChannelRequest
	err := decoder.Decode(&req)

	if err != nil {
		log.Printf("Error decoding request: %s", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	channel, err := h.channelRepo.GetByName(ctx, req.ChannelName)
	if err != nil {
		log.Printf("Error finding channel: %s", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = h.channelRepo.AddUserToChannel(ctx, *channel, *user)
	if err != nil {
		log.Printf("Error adding user to new channel: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	channel.Metadata = internal.FilterMetadata(channel.Metadata, internal.GetNamespace(ctx))

	result, err := json.Marshal(channel)
	if err != nil {
		log.Printf("Error marshaling value: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	joined, err := json.Marshal(channelJoinedEvent{Channel: *channel, User: websockets.User{ID: user.ID, Username: user.Username}})
	if err != nil {
		log.Printf("Error marshaling value: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.hub.Broadcast(websockets.ChannelJoined, joined, []int64{channel.ID})
	writer.WriteHeader(http.StatusOK)
	writer.Write(result)
}

func (h channelHandler) EditChannel(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()

	userRaw := ctx.Value("user")
	user := userRaw.(*users.User)
	if user == nil {
		log.Printf("No user found")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	decoder := json.NewDecoder(request.Body)

	var req editChannelRequest
	err := decoder.Decode(&req)
	if err != nil {
		log.Printf("Error decoding request: %s", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	channel, err := h.channelRepo.GetByID(ctx, req.ID)
	if err != nil {
		log.Printf("Error retrieving channel: %s", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	var meta json.RawMessage
	namespacedMeta := channel.Metadata
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

	channel, err = h.channelRepo.Edit(ctx, *channel, req.Name, meta)
	if err != nil {
		log.Printf("Error editing channel: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	channel.Metadata = internal.FilterMetadata(channel.Metadata, internal.GetNamespace(ctx))

	result, err := json.Marshal(channel)
	if err != nil {
		log.Printf("Error marshaling value: %s", err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	h.hub.Broadcast(websockets.ChannelEdited, result, []int64{channel.ID})
	writer.WriteHeader(http.StatusOK)
	writer.Write(result)
}
