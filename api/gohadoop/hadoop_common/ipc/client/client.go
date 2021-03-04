package ipc

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/mengqi777/ozone-go/api/common"
	"github.com/mengqi777/ozone-go/api/gohadoop/hadoop_common"
	"github.com/mengqi777/ozone-go/api/gohadoop/hadoop_common/security"
	log "github.com/wonderivan/logger"

	"github.com/nu7hatch/gouuid"
	"net"
	"strings"
	"sync"
)

type Client struct {
	ClientId      *uuid.UUID
	Ugi           *hadoop_common.UserInformationProto
	ServerAddress string
	TCPNoDelay    bool
}

type connection struct {
	con net.Conn
}

type connection_id struct {
	user     string
	protocol string
	address  string
}

type call struct {
	callId     int32
	procedure  proto.Message
	request    proto.Message
	response   proto.Message
	err        *error
	retryCount int32
}

func (c *Client) String() string {
	buf := bytes.NewBufferString("")
	fmt.Fprint(buf, "<clientId:", c.ClientId)
	fmt.Fprint(buf, ", server:", c.ServerAddress)
	fmt.Fprint(buf, ">")
	return buf.String()
}

var (
	SASL_RPC_DUMMY_CLIENT_ID     []byte = make([]byte, 0)
	SASL_RPC_CALL_ID             int32  = -33
	SASL_RPC_INVALID_RETRY_COUNT int32  = -1
)

func (c *Client) Call(rpc *hadoop_common.RequestHeaderProto, rpcRequest proto.Message, rpcResponse proto.Message) error {
	// Create connection_id
	connectionId := connection_id{user: *c.Ugi.RealUser, protocol: *rpc.DeclaringClassProtocolName, address: c.ServerAddress}

	// Get connection to server
	//log.Info("Connecting...", c)
	conn, err := getConnection(c, &connectionId)
	if err != nil {
		return err
	}

	// Create call and send request
	rpcCall := call{callId: 0, procedure: rpc, request: rpcRequest, response: rpcResponse}
	err = sendRequest(c, conn, &rpcCall)
	if err != nil {
		log.Error("sendRequest", err)
		return err
	}

	// Read & return response
	err = c.readResponse(conn, &rpcCall)

	return err
}

var connectionPool = struct {
	sync.RWMutex
	connections map[connection_id]*connection
}{connections: make(map[connection_id]*connection)}

func findUsableTokenForService(service string) (*hadoop_common.TokenProto, bool) {
	userTokens := security.GetCurrentUser().GetUserTokens()

	log.Info("looking for token for service: %s\n", service)

	if len(userTokens) == 0 {
		return nil, false
	}

	token := userTokens[service]
	if token != nil {
		return token, true
	}

	return nil, false
}

func getConnection(c *Client, connectionId *connection_id) (*connection, error) {
	// Try to re-use an existing connection
	connectionPool.RLock()
	con := connectionPool.connections[*connectionId]
	connectionPool.RUnlock()
println("host",c.ServerAddress)
	// If necessary, create a new connection and save it in the connection-pool
	var err error
	if con == nil {
		con, err = setupConnection(c)
		if err != nil {
			log.Error("Couldn't setup connection: ", err)
			return nil, err
		}

		connectionPool.Lock()
		connectionPool.connections[*connectionId] = con
		connectionPool.Unlock()

		var authProtocol common.AuthProtocol = common.AUTH_PROTOCOL_NONE

		if _, found := findUsableTokenForService(c.ServerAddress); found {
			log.Info("found token for service: %s", c.ServerAddress)
			authProtocol = common.AUTH_PROTOCOL_SASL
		}

		writeConnectionHeader(con, authProtocol)

		if authProtocol == common.AUTH_PROTOCOL_SASL {
			log.Info("attempting SASL negotiation.")

			if err = negotiateSimpleTokenAuth(c, con); err != nil {
				log.Error("failed to complete SASL negotiation!")
				return nil, err
			}

		} else {
			log.Info("no usable tokens. proceeding without auth.")
		}

		writeConnectionContext(c, con, connectionId, authProtocol)
	}

	return con, nil
}

func setupConnection(c *Client) (*connection, error) {
	addr, err := net.ResolveTCPAddr("tcp", c.ServerAddress)
	if err != nil {
		log.Error("error: ", err)
		return nil, err
	}
	tcpConn, err := net.Dial("tcp",  addr.String())
	if err != nil {
		log.Error("error: ", err)
		return nil, err
	} else {
		log.Info("Successfully connected ", c)
	}

	// TODO: Ping thread

	// Set tcp no-delay
	//tcpConn.SetNoDelay(c.TCPNoDelay)

	return &connection{tcpConn}, nil
}

func writeConnectionHeader(conn *connection, authProtocol common.AuthProtocol) error {
	// RPC_HEADER
	if _, err := conn.con.Write(common.RPC_HEADER); err != nil {
		log.Error("conn.Write gohadoop.RPC_HEADER", err)
		return err
	}

	// RPC_VERSION
	if _, err := conn.con.Write(common.VERSION); err != nil {
		log.Error("conn.Write gohadoop.VERSION", err)
		return err
	}

	// RPC_SERVICE_CLASS
	if serviceClass, err := common.ConvertFixedToBytes(common.RPC_SERVICE_CLASS); err != nil {
		log.Error("binary.Write", err)
		return err
	} else if _, err := conn.con.Write(serviceClass); err != nil {
		log.Error("conn.Write RPC_SERVICE_CLASS", err)
		return err
	}

	// AuthProtocol
	if authProtocolBytes, err := common.ConvertFixedToBytes(authProtocol); err != nil {
		log.Error("WTF AUTH_PROTOCOL", err)
		return err
	} else if _, err := conn.con.Write(authProtocolBytes); err != nil {
		log.Error("conn.Write gohadoop.AUTH_PROTOCOL", err)
		return err
	}

	return nil
}

func writeConnectionContext(c *Client, conn *connection, connectionId *connection_id, authProtocol common.AuthProtocol) error {
	// Create hadoop_common.IpcConnectionContextProto
	ugi, _ := common.CreateSimpleUGIProto()
	ipcCtxProto := hadoop_common.IpcConnectionContextProto{UserInfo: ugi, Protocol: &connectionId.protocol}

	// Create RpcRequestHeaderProto
	var callId int32 = -3
	var clientId [16]byte = [16]byte(*c.ClientId)

	/*if (authProtocol == gohadoop.AUTH_PROTOCOL_SASL) {
	  callId = SASL_RPC_CALL_ID
	}*/

	rpcReqHeaderProto := hadoop_common.RpcRequestHeaderProto{RpcKind: &common.RPC_PROTOCOL_BUFFFER, RpcOp: &common.RPC_FINAL_PACKET, CallId: &callId, ClientId: clientId[0:16], RetryCount: &common.RPC_DEFAULT_RETRY_COUNT}

	rpcReqHeaderProtoBytes, err := proto.Marshal(&rpcReqHeaderProto)
	if err != nil {
		log.Error("proto.Marshal(&rpcReqHeaderProto)", err)
		return err
	}

	ipcCtxProtoBytes, _ := proto.Marshal(&ipcCtxProto)
	if err != nil {
		log.Error("proto.Marshal(&ipcCtxProto)", err)
		return err
	}

	totalLength := len(rpcReqHeaderProtoBytes) + sizeVarint(len(rpcReqHeaderProtoBytes)) + len(ipcCtxProtoBytes) + sizeVarint(len(ipcCtxProtoBytes))
	var tLen int32 = int32(totalLength)
	totalLengthBytes, err := common.ConvertFixedToBytes(tLen)

	if err != nil {
		log.Error("ConvertFixedToBytes(totalLength)", err)
		return err
	} else if _, err := conn.con.Write(totalLengthBytes); err != nil {
		log.Error("conn.con.Write(totalLengthBytes)", err)
		return err
	}

	if err := writeDelimitedBytes(conn, rpcReqHeaderProtoBytes); err != nil {
		log.Error("writeDelimitedBytes(conn, rpcReqHeaderProtoBytes)", err)
		return err
	}
	if err := writeDelimitedBytes(conn, ipcCtxProtoBytes); err != nil {
		log.Error("writeDelimitedBytes(conn, ipcCtxProtoBytes)", err)
		return err
	}

	return nil
}

func sizeVarint(x int) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func sendRequest(c *Client, conn *connection, rpcCall *call) error {
	//log.Info("About to call RPC: ", rpcCall.procedure)

	// 0. RpcRequestHeaderProto
	var clientId [16]byte = [16]byte(*c.ClientId)
	rpcReqHeaderProto := hadoop_common.RpcRequestHeaderProto{RpcKind: &common.RPC_PROTOCOL_BUFFFER, RpcOp: &common.RPC_FINAL_PACKET, CallId: &rpcCall.callId, ClientId: clientId[0:16], RetryCount: &rpcCall.retryCount}
	rpcReqHeaderProtoBytes, err := proto.Marshal(&rpcReqHeaderProto)
	if err != nil {
		log.Error("proto.Marshal(&rpcReqHeaderProto)", err)
		return err
	}

	// 1. RequestHeaderProto
	requestHeaderProto := rpcCall.procedure
	requestHeaderProtoBytes, err := proto.Marshal(requestHeaderProto)
	if err != nil {
		log.Error("proto.Marshal(&requestHeaderProto)", err)
		return err
	}

	// 2. Param
	paramProto := rpcCall.request
	paramProtoBytes, err := proto.Marshal(paramProto)
	if err != nil {
		log.Error("proto.Marshal(&paramProto)", err)
		return err
	}

	totalLength := len(rpcReqHeaderProtoBytes) + sizeVarint(len(rpcReqHeaderProtoBytes)) + len(requestHeaderProtoBytes) + sizeVarint(len(requestHeaderProtoBytes)) + len(paramProtoBytes) + sizeVarint(len(paramProtoBytes))
	var tLen int32 = int32(totalLength)
	if totalLengthBytes, err := common.ConvertFixedToBytes(tLen); err != nil {
		log.Error("ConvertFixedToBytes(totalLength)", err)
		return err
	} else {
		if _, err := conn.con.Write(totalLengthBytes); err != nil {
			log.Error("conn.con.Write(totalLengthBytes)", err)
			return err
		}
	}

	if err := writeDelimitedBytes(conn, rpcReqHeaderProtoBytes); err != nil {
		log.Error("writeDelimitedBytes(conn, rpcReqHeaderProtoBytes)", err)
		return err
	}
	if err := writeDelimitedBytes(conn, requestHeaderProtoBytes); err != nil {
		log.Error("writeDelimitedBytes(conn, requestHeaderProtoBytes)", err)
		return err
	}
	if err := writeDelimitedBytes(conn, paramProtoBytes); err != nil {
		log.Error("writeDelimitedBytes(conn, paramProtoBytes)", err)
		return err
	}

	//log.Info("Succesfully sent request of length: ", totalLength)

	return nil
}

func writeDelimitedTo(conn *connection, msg proto.Message) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		log.Error("proto.Marshal(msg)", err)
		return err
	}
	return writeDelimitedBytes(conn, msgBytes)
}

func writeDelimitedBytes(conn *connection, data []byte) error {
	if _, err := conn.con.Write(proto.EncodeVarint(uint64(len(data)))); err != nil {
		log.Error("conn.con.Write(proto.EncodeVarint(uint64(len(data))))", err)
		return err
	}
	if _, err := conn.con.Write(data); err != nil {
		log.Error("conn.con.Write(data)", err)
		return err
	}

	return nil
}

func (c *Client) readResponse(conn *connection, rpcCall *call) error {
	// Read first 4 bytes to get total-length
	var totalLength int32 = -1
	var totalLengthBytes [4]byte
	if _, err := conn.con.Read(totalLengthBytes[0:4]); err != nil {
		log.Error("conn.con.Read(totalLengthBytes)", err)
		return err
	}

	if err := common.ConvertBytesToFixed(totalLengthBytes[0:4], &totalLength); err != nil {
		log.Error("gohadoop.ConvertBytesToFixed(totalLengthBytes, &totalLength)", err)
		return err
	}

	var responseBytes = make([]byte,0,totalLength)
	remain := totalLength
	tmp := make([]byte, 1024)
	for remain > 0 {
		n, err := conn.con.Read(tmp)
		if err != nil {
			return err
		}
		remain = remain - int32(n)
		responseBytes = append(responseBytes, tmp[:n]...)
	}
	

	// Parse RpcResponseHeaderProto
	rpcResponseHeaderProto := hadoop_common.RpcResponseHeaderProto{}
	off, err := readDelimited(responseBytes[0:totalLength], &rpcResponseHeaderProto)
	if err != nil {
		log.Error("readDelimited(responseBytes, rpcResponseHeaderProto)", err)
		return err
	}
	//log.Info("Received rpcResponseHeaderProto = ", rpcResponseHeaderProto)

	err = c.checkRpcHeader(&rpcResponseHeaderProto)
	if err != nil {
		log.Error("c.checkRpcHeader failed", err)
		return err
	}

	if *rpcResponseHeaderProto.Status == hadoop_common.RpcResponseHeaderProto_SUCCESS {
		// Parse RpcResponseWrapper
		_, err = readDelimited(responseBytes[off:], rpcCall.response)
	} else {
		log.Info("RPC failed with status: ", rpcResponseHeaderProto.Status.String())
		errorDetails := [4]string{rpcResponseHeaderProto.Status.String(), "ServerDidNotSetExceptionClassName", "ServerDidNotSetErrorMsg", "ServerDidNotSetErrorDetail"}
		if rpcResponseHeaderProto.ExceptionClassName != nil {
			errorDetails[0] = *rpcResponseHeaderProto.ExceptionClassName
		}
		if rpcResponseHeaderProto.ErrorMsg != nil {
			errorDetails[1] = *rpcResponseHeaderProto.ErrorMsg
		}
		if rpcResponseHeaderProto.ErrorDetail != nil {
			errorDetails[2] = rpcResponseHeaderProto.ErrorDetail.String()
		}
		err = errors.New(strings.Join(errorDetails[:], ":"))
	}
	return err
}

func readDelimited(rawData []byte, msg proto.Message) (int, error) {
	headerLength, off := proto.DecodeVarint(rawData)
	if off == 0 {
		log.Error("proto.DecodeVarint(rawData) returned zero")
		return -1, nil
	}
	err := proto.Unmarshal(rawData[off:off+int(headerLength)], msg)
	if err != nil {
		log.Error("proto.Unmarshal(rawData[off:off+headerLength]) ", err)
		return -1, err
	}

	return off + int(headerLength), nil
}

func (c *Client) checkRpcHeader(rpcResponseHeaderProto *hadoop_common.RpcResponseHeaderProto) error {
	var callClientId [16]byte = [16]byte(*c.ClientId)
	var headerClientId []byte = []byte(rpcResponseHeaderProto.ClientId)
	if rpcResponseHeaderProto.ClientId != nil {
		if !bytes.Equal(callClientId[0:16], headerClientId[0:16]) {
			log.Error("Incorrect clientId: ", headerClientId)
			return errors.New("Incorrect clientId")
		}
	}
	return nil
}

func sendSaslMessage(c *Client, conn *connection, message *hadoop_common.RpcSaslProto) error {
	saslRpcHeaderProto := hadoop_common.RpcRequestHeaderProto{RpcKind: &common.RPC_PROTOCOL_BUFFFER,
		RpcOp:      &common.RPC_FINAL_PACKET,
		CallId:     &SASL_RPC_CALL_ID,
		ClientId:   SASL_RPC_DUMMY_CLIENT_ID,
		RetryCount: &SASL_RPC_INVALID_RETRY_COUNT}

	saslRpcHeaderProtoBytes, err := proto.Marshal(&saslRpcHeaderProto)

	if err != nil {
		log.Error("proto.Marshal(&saslRpcHeaderProto)", err)
		return err
	}

	saslRpcMessageProtoBytes, err := proto.Marshal(message)

	if err != nil {
		log.Error("proto.Marshal(saslMessage)", err)
		return err
	}

	totalLength := len(saslRpcHeaderProtoBytes) + sizeVarint(len(saslRpcHeaderProtoBytes)) + len(saslRpcMessageProtoBytes) + sizeVarint(len(saslRpcMessageProtoBytes))
	var tLen int32 = int32(totalLength)

	if totalLengthBytes, err := common.ConvertFixedToBytes(tLen); err != nil {
		log.Error("ConvertFixedToBytes(totalLength)", err)
		return err
	} else {
		if _, err := conn.con.Write(totalLengthBytes); err != nil {
			log.Error("conn.con.Write(totalLengthBytes)", err)
			return err
		}
	}
	if err := writeDelimitedBytes(conn, saslRpcHeaderProtoBytes); err != nil {
		log.Error("writeDelimitedBytes(conn, saslRpcHeaderProtoBytes)", err)
		return err
	}
	if err := writeDelimitedBytes(conn, saslRpcMessageProtoBytes); err != nil {
		log.Error("writeDelimitedBytes(conn, saslRpcMessageProtoBytes)", err)
		return err
	}

	return nil
}

func receiveSaslMessage(c *Client, conn *connection) (*hadoop_common.RpcSaslProto, error) {
	// Read first 4 bytes to get total-length
	var totalLength int32 = -1
	var totalLengthBytes [4]byte

	if _, err := conn.con.Read(totalLengthBytes[0:4]); err != nil {
		log.Error("conn.con.Read(totalLengthBytes)", err)
		return nil, err
	}
	if err := common.ConvertBytesToFixed(totalLengthBytes[0:4], &totalLength); err != nil {
		log.Error("gohadoop.ConvertBytesToFixed(totalLengthBytes, &totalLength)", err)
		return nil, err
	}

	var responseBytes []byte = make([]byte, totalLength)

	if _, err := conn.con.Read(responseBytes); err != nil {
		log.Error("conn.con.Read(totalLengthBytes)", err)
		return nil, err
	}

	// Parse RpcResponseHeaderProto
	rpcResponseHeaderProto := hadoop_common.RpcResponseHeaderProto{}
	off, err := readDelimited(responseBytes[0:totalLength], &rpcResponseHeaderProto)
	if err != nil {
		log.Error("readDelimited(responseBytes, rpcResponseHeaderProto)", err)
		return nil, err
	}

	err = checkSaslRpcHeader(&rpcResponseHeaderProto)
	if err != nil {
		log.Error("checkSaslRpcHeader failed", err)
		return nil, err
	}

	var saslRpcMessage hadoop_common.RpcSaslProto

	if *rpcResponseHeaderProto.Status == hadoop_common.RpcResponseHeaderProto_SUCCESS {
		// Parse RpcResponseWrapper
		if _, err = readDelimited(responseBytes[off:], &saslRpcMessage); err != nil {
			log.Error("failed to read sasl response!")
			return nil, err
		} else {
			return &saslRpcMessage, nil
		}
	} else {
		log.Info("RPC failed with status: ", rpcResponseHeaderProto.Status.String())
		errorDetails := [4]string{rpcResponseHeaderProto.Status.String(), "ServerDidNotSetExceptionClassName", "ServerDidNotSetErrorMsg", "ServerDidNotSetErrorDetail"}
		if rpcResponseHeaderProto.ExceptionClassName != nil {
			errorDetails[0] = *rpcResponseHeaderProto.ExceptionClassName
		}
		if rpcResponseHeaderProto.ErrorMsg != nil {
			errorDetails[1] = *rpcResponseHeaderProto.ErrorMsg
		}
		if rpcResponseHeaderProto.ErrorDetail != nil {
			errorDetails[2] = rpcResponseHeaderProto.ErrorDetail.String()
		}
		err = errors.New(strings.Join(errorDetails[:], ":"))
		return nil, err
	}
}

func checkSaslRpcHeader(rpcResponseHeaderProto *hadoop_common.RpcResponseHeaderProto) error {
	var headerClientId []byte = []byte(rpcResponseHeaderProto.ClientId)
	if rpcResponseHeaderProto.ClientId != nil {
		if !bytes.Equal(SASL_RPC_DUMMY_CLIENT_ID, headerClientId) {
			log.Error("Incorrect clientId: ", headerClientId)
			return errors.New("Incorrect clientId")
		}
	}
	return nil
}

func negotiateSimpleTokenAuth(client *Client, con *connection) error {
	var saslNegotiateState hadoop_common.RpcSaslProto_SaslState = hadoop_common.RpcSaslProto_NEGOTIATE
	var saslNegotiateMessage hadoop_common.RpcSaslProto = hadoop_common.RpcSaslProto{State: &saslNegotiateState}
	var saslResponseMessage *hadoop_common.RpcSaslProto
	var err error

	//send a SASL negotiation request
	if err = sendSaslMessage(client, con, &saslNegotiateMessage); err != nil {
		log.Error("failed to send SASL NEGOTIATE message!")
		return err
	}

	//get a response with supported mehcanisms/challenge
	if saslResponseMessage, err = receiveSaslMessage(client, con); err != nil {
		log.Error("failed to receive SASL NEGOTIATE response!")
		return err
	}

	var auths []*hadoop_common.RpcSaslProto_SaslAuth = saslResponseMessage.GetAuths()

	if numAuths := len(auths); numAuths <= 0 {
		log.Error("No supported auth mechanisms!")
		return errors.New("No supported auth mechanisms!")
	}

	//for now we only support auth when TOKEN/DIGEST-MD5 is the first/only
	//supported auth mechanism
	var auth *hadoop_common.RpcSaslProto_SaslAuth = auths[0]

	if !(auth.GetMethod() == "TOKEN" && auth.GetMechanism() == "DIGEST-MD5") {
		log.Error("gohadoop only supports TOKEN/DIGEST-MD5 auth!")
		return errors.New("gohadoop only supports TOKEN/DIGEST-MD5 auth!")
	}

	method := auth.GetMethod()
	mechanism := auth.GetMechanism()
	protocol := auth.GetProtocol()
	serverId := auth.GetServerId()
	challenge := auth.GetChallenge()

	//TODO: token/service mapping + token selection based on type/service
	//we wouldn't have gotten this far if there wasn't at least one available token.
	userToken, _ := findUsableTokenForService(client.ServerAddress)
	response, err := security.GetDigestMD5ChallengeResponse(protocol, serverId, challenge, userToken)

	if err != nil {
		log.Error("failed to get challenge response! ", err)
		return err
	}

	saslInitiateState := hadoop_common.RpcSaslProto_INITIATE
	authSend := hadoop_common.RpcSaslProto_SaslAuth{Method: &method, Mechanism: &mechanism,
		Protocol: &protocol, ServerId: &serverId}
	authsSendArray := []*hadoop_common.RpcSaslProto_SaslAuth{&authSend}
	saslInitiateMessage := hadoop_common.RpcSaslProto{State: &saslInitiateState,
		Token: []byte(response), Auths: authsSendArray}

	//send a SASL inititate request
	if err = sendSaslMessage(client, con, &saslInitiateMessage); err != nil {
		log.Error("failed to send SASL INITIATE message!")
		return err
	}

	//get a response with supported mehcanisms/challenge
	if saslResponseMessage, err = receiveSaslMessage(client, con); err != nil {
		log.Error("failed to read response to SASL INITIATE response!")
		return err
	}

	if saslResponseMessage.GetState() != hadoop_common.RpcSaslProto_SUCCESS {
		log.Error("expected SASL SUCCESS response!")
		return errors.New("expected SASL SUCCESS response!")
	}

	log.Info("Successfully completed SASL negotiation!")

	return nil //errors.New("abort here")
}
