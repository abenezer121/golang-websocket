package socket

import (
	"encoding/json"
	"fastsocket/config"
	"fastsocket/models"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
	"log"
)

func SendErrorText(conn *websocket.Conn, message string) error {
	if config.ResponseFormat == "proto" {
		errMsg := models.ErrorMessageProto{
			Error: message,
		}
		errResp, err := proto.Marshal(&errMsg)
		if err != nil {
			return err
		}
		if writeErr := conn.WriteMessage(websocket.BinaryMessage, errResp); writeErr != nil {
			return writeErr
		}

	} else {

		errMsg := []byte(`{"error": ` + message + `}`)
		if writeErr := conn.WriteMessage(websocket.TextMessage, errMsg); writeErr != nil {
			return writeErr
		}
	}

	return nil
}

// Todo Or send an internal server error message
func SendDriverDataProto(conn *websocket.Conn, paginated []*models.CommandProto, errMsg string) error {
	newSocketProtoResponse := models.SocketResponseProto{
		Command:   "get-drivers",
		Paginated: paginated,
	}
	respBytes, marshalErr := proto.Marshal(&newSocketProtoResponse)

	if marshalErr != nil {
		log.Printf("ERROR: "+errMsg+" %s: %v", conn.RemoteAddr(), marshalErr)

		return marshalErr
	}
	if writeErr := conn.WriteMessage(websocket.BinaryMessage, respBytes); writeErr != nil {
		log.Printf("ERROR: "+errMsg+" %s: %v", conn.RemoteAddr(), writeErr)
		return writeErr
	}
	return nil
}

func SendDriverData(conn *websocket.Conn, paginated []models.Command, errMsg string) error {
	newSocketResponse := models.SocketResponse{Command: "get-bbox", Paginated: paginated}

	responseBytes, marshalErr := json.Marshal(newSocketResponse)
	if marshalErr != nil {
		log.Printf("ERROR: "+errMsg+" %s: %v", conn.RemoteAddr(), marshalErr)
		//Todo Or send an internal server error message
		return marshalErr
	}
	if writeErr := conn.WriteMessage(websocket.TextMessage, responseBytes); writeErr != nil {
		log.Printf("ERROR: "+errMsg+" %s: %v", conn.RemoteAddr(), writeErr)
		return writeErr
	}
	return nil
}
