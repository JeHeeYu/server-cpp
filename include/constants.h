#pragma once

namespace socketIoServer::constants {

inline constexpr const char* kHttpStatusOk = "200 OK";
inline constexpr const char* kHttpStatusBadRequest = "400 Bad Request";
inline constexpr const char* kHttpStatusNotFound = "404 Not Found";
inline constexpr const char* kHttpStatusMethodNotAllowed = "405 Method Not Allowed";
inline constexpr const char* kHttpStatusPayloadTooLarge = "413 Payload Too Large";

inline constexpr const char* kBodyAlive = "socketIoServerCpp alive\n";
inline constexpr const char* kBodyNotFound = "not found";
inline constexpr const char* kBodyMissingQuery = "missing query";
inline constexpr const char* kBodyUnsupportedTransport = "unsupported transport";
inline constexpr const char* kBodyMissingSid = "missing sid";
inline constexpr const char* kBodyUnknownSid = "unknown sid";
inline constexpr const char* kBodyOk = "ok";
inline constexpr const char* kBodyMethodNotAllowed = "method not allowed";
inline constexpr const char* kBodyTooManyPackets = "too many packets in single post";

inline constexpr int kCodeMalformedConnectPacket = 4001;
inline constexpr int kCodeMalformedEventPacket = 4002;
inline constexpr int kCodeNamespaceNotConnected = 4003;
inline constexpr int kCodeEmptyEventName = 4004;
inline constexpr int kCodeBinaryAttachmentOutOfOrder = 4006;
inline constexpr int kCodeBinaryAttachmentCountMismatch = 4007;
inline constexpr int kCodeOutgoingQueueOverflow = 4008;
inline constexpr int kCodePayloadTooLarge = 4009;
inline constexpr int kCodeTooManyBinaryAttachments = 4010;
inline constexpr int kCodeRateLimitExceeded = 4011;

inline constexpr const char* kMessageMalformedConnectPacket = "malformed connect packet";
inline constexpr const char* kMessageMalformedEventPacket = "malformed event packet";
inline constexpr const char* kMessageNamespaceNotConnected = "namespace not connected";
inline constexpr const char* kMessageEmptyEventName = "empty event name";
inline constexpr const char* kMessageBinaryAttachmentOutOfOrder = "binary attachment out of order";
inline constexpr const char* kMessageBinaryAttachmentCountMismatch = "binary attachment count mismatch";
inline constexpr const char* kMessageOutgoingQueueOverflow = "outgoing queue overflow";
inline constexpr const char* kMessagePayloadTooLarge = "payload too large";
inline constexpr const char* kMessageTooManyBinaryAttachments = "too many binary attachments";
inline constexpr const char* kMessageRateLimitExceeded = "rate limit exceeded";

}  // namespace socketIoServer::constants
