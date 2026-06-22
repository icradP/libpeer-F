#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "agent.h"
#include "config.h"
#include "dtls_srtp.h"
#include "peer_connection.h"
#include "ports.h"
#include "rtcp.h"
#include "rtp.h"
#include "sctp.h"
#include "sdp.h"

#ifndef MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY
#define MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY -0x7880
#endif

#ifndef MBEDTLS_ERR_SSL_WANT_READ
#define MBEDTLS_ERR_SSL_WANT_READ -0x6900
#endif

#ifndef MBEDTLS_ERR_SSL_INVALID_RECORD
#define MBEDTLS_ERR_SSL_INVALID_RECORD -0x7200
#endif

#ifndef MBEDTLS_ERR_SSL_BAD_HS_PROTOCOL_VERSION
#define MBEDTLS_ERR_SSL_BAD_HS_PROTOCOL_VERSION -0x7300
#endif

#define PEER_CONNECTION_DTLS_RX_SIZE 8192
#define PEER_CONNECTION_DTLS_FLIGHT_MAX_IDLE 40

#define STATE_CHANGED(pc, curr_state)                                 \
  {                                                                   \
    if (pc->oniceconnectionstatechange && pc->state != curr_state) {  \
      pc->oniceconnectionstatechange(curr_state, pc->config.user_data); \
    }                                                                 \
    pc->state = curr_state;                                           \
  }

typedef enum RemoteDtlsSetup {
  REMOTE_DTLS_SETUP_UNKNOWN = 0,
  REMOTE_DTLS_SETUP_ACTPASS,
  REMOTE_DTLS_SETUP_ACTIVE,
  REMOTE_DTLS_SETUP_PASSIVE,
} RemoteDtlsSetup;

struct PeerConnection {
  PeerConfiguration config;
  PeerConnectionState state;
  Agent agent;
  DtlsSrtp dtls_srtp;
  Sctp sctp;

  char sdp[CONFIG_SDP_BUFFER_SIZE];

  void (*onicecandidate)(char* sdp, void* user_data);
  void (*oniceconnectionstatechange)(PeerConnectionState state, void* user_data);
  void (*on_connected)(void* userdata);
  void (*on_receiver_packet_loss)(float fraction_loss, uint32_t total_loss, void* user_data);

  uint8_t temp_buf[CONFIG_MTU];
  uint8_t agent_buf[CONFIG_MTU];
  int agent_ret;
  int b_local_description_created;

  RtpEncoder artp_encoder;
  RtpEncoder vrtp_encoder;
  RtpDecoder vrtp_decoder;
  RtpDecoder artp_decoder;

  uint32_t remote_assrc;
  uint32_t remote_vssrc;
  uint32_t local_assrc;
  uint32_t local_vssrc;

  uint32_t local_vpackets;
  uint32_t local_voctets;
  uint32_t local_apackets;
  uint32_t local_aoctets;
  uint32_t last_rtcp_sr_ms;
  uint32_t last_rtcp_rr_ms;
  int dtls_handshake_started;
  int rtcp_bye_sent;
  int dtls_hs_want_count;
  RemoteDtlsSetup remote_dtls_setup;
  int talk_dtls_client_offer;
  uint32_t last_dtls_hs_ms;
  uint8_t dtls_rx_buf[PEER_CONNECTION_DTLS_RX_SIZE];
  size_t dtls_rx_len;
};

static void peer_connection_outgoing_rtcp_packet(PeerConnection* pc, uint8_t* data, size_t size) {
  dtls_srtp_encrypt_rctp_packet(&pc->dtls_srtp, data, (int*)&size);
  agent_send(&pc->agent, data, size);
}

static void peer_connection_maybe_send_rtcp_sr(PeerConnection* pc) {
  uint32_t now = ports_get_epoch_time();
  uint8_t rtcp_buf[128];
  int len;

  if (pc->last_rtcp_sr_ms != 0 && (now - pc->last_rtcp_sr_ms) < 5000)
    return;

  if (pc->config.video_codec != CODEC_NONE && pc->local_vpackets > 0) {
    len = rtcp_build_sr(rtcp_buf, sizeof(rtcp_buf), pc->vrtp_encoder.ssrc, pc->vrtp_encoder.timestamp, pc->local_vpackets, pc->local_voctets);
    if (len > 0)
      peer_connection_outgoing_rtcp_packet(pc, rtcp_buf, len);
  }

  if (pc->config.audio_codec != CODEC_NONE && pc->local_apackets > 0) {
    len = rtcp_build_sr(rtcp_buf, sizeof(rtcp_buf), pc->artp_encoder.ssrc, pc->artp_encoder.timestamp, pc->local_apackets, pc->local_aoctets);
    if (len > 0)
      peer_connection_outgoing_rtcp_packet(pc, rtcp_buf, len);
  }

  pc->last_rtcp_sr_ms = now;
}

static void peer_connection_maybe_send_rtcp_rr(PeerConnection* pc) {
  uint32_t now = ports_get_epoch_time();
  uint8_t rtcp_buf[128];
  int len;

  if (pc->last_rtcp_rr_ms != 0 && (now - pc->last_rtcp_rr_ms) < 5000)
    return;

  if (pc->config.audio_codec != CODEC_NONE && pc->remote_assrc != 0) {
    len = rtcp_build_rr(rtcp_buf, sizeof(rtcp_buf), pc->local_assrc, pc->remote_assrc);
    if (len > 0)
      peer_connection_outgoing_rtcp_packet(pc, rtcp_buf, len);
  }

  if (pc->config.video_codec != CODEC_NONE && pc->remote_vssrc != 0) {
    len = rtcp_build_rr(rtcp_buf, sizeof(rtcp_buf), pc->local_vssrc, pc->remote_vssrc);
    if (len > 0)
      peer_connection_outgoing_rtcp_packet(pc, rtcp_buf, len);
  }

  pc->last_rtcp_rr_ms = now;
}

static void peer_connection_send_rtcp_bye(PeerConnection* pc) {
  uint8_t rtcp_buf[128];
  int len;

  if (pc->rtcp_bye_sent || pc->state != PEER_CONNECTION_COMPLETED) {
    return;
  }

  if (pc->config.audio_codec != CODEC_NONE) {
    len = rtcp_build_bye(rtcp_buf, sizeof(rtcp_buf), pc->local_assrc);
    if (len > 0)
      peer_connection_outgoing_rtcp_packet(pc, rtcp_buf, len);
  }

  if (pc->config.video_codec != CODEC_NONE) {
    len = rtcp_build_bye(rtcp_buf, sizeof(rtcp_buf), pc->local_vssrc);
    if (len > 0)
      peer_connection_outgoing_rtcp_packet(pc, rtcp_buf, len);
  }

  pc->rtcp_bye_sent = 1;
}

static void peer_connection_outgoing_rtp_packet(uint8_t* data, size_t size, void* user_data) {
  PeerConnection* pc = (PeerConnection*)user_data;
  uint32_t ssrc = rtp_get_ssrc(data);

  if (ssrc == pc->vrtp_encoder.ssrc) {
    pc->local_vpackets++;
    pc->local_voctets += size;
  } else if (ssrc == pc->artp_encoder.ssrc) {
    pc->local_apackets++;
    pc->local_aoctets += size;
  }

  dtls_srtp_encrypt_rtp_packet(&pc->dtls_srtp, data, (int*)&size);
  agent_send(&pc->agent, data, size);
  peer_connection_maybe_send_rtcp_sr(pc);
}

static int peer_connection_datagram_is_dtls(const uint8_t* buf, size_t len) {
  if (buf == NULL || len < 1) {
    return 0;
  }

  return buf[0] >= 20 && buf[0] <= 63;
}

static void peer_connection_dtls_rx_clear(PeerConnection* pc) {
  pc->dtls_rx_len = 0;
}

static int peer_connection_dtls_client_hello_complete(const uint8_t* buf, size_t len) {
  size_t off = 0;
  uint16_t hello_seq = 0;
  uint32_t hello_total = 0;
  uint32_t hello_end = 0;
  int seen = 0;

  while (off + 13 <= len) {
    uint8_t ctype = buf[off];
    uint16_t rec_len = ((uint16_t)buf[off + 11] << 8) | buf[off + 12];

    if (off + 13 + rec_len > len) {
      return 0;
    }

    if (ctype == 22 && rec_len >= 12) {
      const uint8_t* hs = buf + off + 13;
      if (hs[0] == 1) {
        uint32_t hs_len = ((uint32_t)hs[1] << 16) | ((uint32_t)hs[2] << 8) | hs[3];
        uint16_t seq = ((uint16_t)hs[4] << 8) | hs[5];
        uint32_t frag_off = ((uint32_t)hs[6] << 16) | ((uint32_t)hs[7] << 8) | hs[8];
        uint32_t frag_len = ((uint32_t)hs[9] << 16) | ((uint32_t)hs[10] << 8) | hs[11];

        if (!seen) {
          hello_seq = seq;
          hello_total = hs_len;
          seen = 1;
        }

        if (seq == hello_seq) {
          uint32_t end = frag_off + frag_len;
          if (end > hello_end) {
            hello_end = end;
          }
        }
      }
    }

    off += 13 + rec_len;
  }

  if (!seen) {
    return len > 0;
  }

  return hello_end >= hello_total;
}

static int peer_connection_dtls_has_client_hello(const uint8_t* buf, size_t len) {
  size_t off = 0;

  while (off + 13 <= len) {
    uint8_t ctype = buf[off];
    uint16_t rec_len = ((uint16_t)buf[off + 11] << 8) | buf[off + 12];

    if (off + 13 + rec_len > len) {
      break;
    }

    if (ctype == 22 && rec_len >= 1 && buf[off + 13] == 1) {
      return 1;
    }

    off += 13 + rec_len;
  }

  return 0;
}

static int peer_connection_dtls_should_buffer_more(const uint8_t* buf, size_t len) {
  if (!peer_connection_dtls_has_client_hello(buf, len)) {
    return 0;
  }

  return !peer_connection_dtls_client_hello_complete(buf, len);
}

static void peer_connection_dtls_drain_flight(PeerConnection* pc) {
  uint8_t tmp[CONFIG_MTU];
  int idle = 0;

  while (idle < PEER_CONNECTION_DTLS_FLIGHT_MAX_IDLE) {
    if (pc->dtls_rx_len > 0 &&
        peer_connection_dtls_client_hello_complete(pc->dtls_rx_buf, pc->dtls_rx_len)) {
      break;
    }

    int ret = agent_recv_from_selected(&pc->agent, tmp, sizeof(tmp));
    if (ret <= 0) {
      idle++;
      continue;
    }

    if (!peer_connection_datagram_is_dtls(tmp, (size_t)ret)) {
      continue;
    }

    if (pc->dtls_rx_len + (size_t)ret > PEER_CONNECTION_DTLS_RX_SIZE) {
      LOGE("DTLS RX buffer overflow (flight %zu + %d)", pc->dtls_rx_len, ret);
      break;
    }

    memcpy(pc->dtls_rx_buf + pc->dtls_rx_len, tmp, ret);
    pc->dtls_rx_len += (size_t)ret;
    LOGD("DTLS RX: flight append len=%d buffered=%zu", ret, pc->dtls_rx_len);
    idle = 0;
  }
}

static int peer_connection_dtls_handshake_retriable(int hs_ret) {
  if (hs_ret == MBEDTLS_ERR_SSL_WANT_READ || hs_ret == MBEDTLS_ERR_SSL_WANT_WRITE) {
    return 1;
  }

  if (hs_ret == MBEDTLS_ERR_SSL_INVALID_RECORD ||
      hs_ret == MBEDTLS_ERR_SSL_BAD_HS_PROTOCOL_VERSION) {
    return 1;
  }

  return 0;
}

static int peer_connection_dtls_srtp_recv(void* ctx, unsigned char* buf, size_t len) {
  int ret = -1;
  uint8_t tmp[CONFIG_MTU];
  DtlsSrtp* dtls_srtp = (DtlsSrtp*)ctx;
  PeerConnection* pc = (PeerConnection*)dtls_srtp->user_data;

  if (pc->agent_ret > 0 && pc->agent_ret <= (int)len) {
    memcpy(buf, pc->agent_buf, pc->agent_ret);
    ret = pc->agent_ret;
    pc->agent_ret = 0;
    return ret;
  }

  if (pc->state == PEER_CONNECTION_CONNECTED) {
    if (pc->dtls_srtp.role == DTLS_SRTP_ROLE_SERVER) {
      if (pc->dtls_rx_len == 0) {
        peer_connection_dtls_drain_flight(pc);
      }
      while (pc->dtls_rx_len > 0 &&
             peer_connection_dtls_should_buffer_more(pc->dtls_rx_buf, pc->dtls_rx_len)) {
        peer_connection_dtls_drain_flight(pc);
      }
    } else if (pc->dtls_rx_len == 0) {
      ret = agent_recv_from_selected(&pc->agent, tmp, sizeof(tmp));
      if (ret > 0 && peer_connection_datagram_is_dtls(tmp, (size_t)ret)) {
        if ((size_t)ret <= len) {
          memcpy(buf, tmp, ret);
          LOGD("DTLS RX: len=%d content_type=%u", ret, tmp[0]);
          return ret;
        }
        if ((size_t)ret <= PEER_CONNECTION_DTLS_RX_SIZE) {
          memcpy(pc->dtls_rx_buf, tmp, ret);
          pc->dtls_rx_len = (size_t)ret;
        }
      }
    }
  }

  if (pc->dtls_rx_len > 0) {
    if (pc->dtls_srtp.role == DTLS_SRTP_ROLE_SERVER &&
        peer_connection_dtls_should_buffer_more(pc->dtls_rx_buf, pc->dtls_rx_len)) {
      return MBEDTLS_ERR_SSL_WANT_READ;
    }

    size_t n = pc->dtls_rx_len;
    if (n > len) {
      n = len;
    }
    memcpy(buf, pc->dtls_rx_buf, n);
    pc->dtls_rx_len -= n;
    if (pc->dtls_rx_len > 0) {
      memmove(pc->dtls_rx_buf, pc->dtls_rx_buf + n, pc->dtls_rx_len);
    }
    LOGD("DTLS RX: deliver len=%zu content_type=%u remain=%zu", n, buf[0], pc->dtls_rx_len);
    return (int)n;
  }

  return MBEDTLS_ERR_SSL_WANT_READ;
}

static int peer_connection_dtls_srtp_send(void* ctx, const uint8_t* buf, size_t len) {
  DtlsSrtp* dtls_srtp = (DtlsSrtp*)ctx;
  PeerConnection* pc = (PeerConnection*)dtls_srtp->user_data;

  if (pc->state == PEER_CONNECTION_CONNECTED && len > 0 && pc->dtls_hs_want_count <= 1) {
    LOGI("DTLS TX: len=%zu content_type=%u", len, buf[0]);
  }

  return agent_send(&pc->agent, buf, len);
}

static void peer_connection_log_dtls_record(const uint8_t* buf, int len) {
  if (buf == NULL || len < 1) {
    return;
  }

  if (len >= 13) {
    uint16_t epoch = ((uint16_t)buf[3] << 8) | buf[4];
    uint16_t record_len = ((uint16_t)buf[11] << 8) | buf[12];
    LOGW("Got DTLS record: type=%u version=%02x%02x epoch=%u record_len=%u packet_len=%d",
         buf[0], buf[1], buf[2], epoch, record_len, len);
  } else {
    LOGW("Got short DTLS record: type=%u packet_len=%d", buf[0], len);
  }
}

static void peer_connection_handle_dtls_record(PeerConnection* pc) {
  int ret;

  peer_connection_log_dtls_record(pc->agent_buf, pc->agent_ret);

  ret = dtls_srtp_read(&pc->dtls_srtp, pc->temp_buf, sizeof(pc->temp_buf));
  if (ret == MBEDTLS_ERR_SSL_PEER_CLOSE_NOTIFY) {
    LOGI("Got DTLS close_notify");
    STATE_CHANGED(pc, PEER_CONNECTION_CLOSED);
    return;
  }

  LOGD("Got DTLS data %d", ret);
  if (ret > 0) {
    sctp_incoming_data(&pc->sctp, (char*)pc->temp_buf, ret);
  }
}

static uint32_t peer_connection_read_u32_be(const uint8_t* buf) {
  return ((uint32_t)buf[0] << 24) |
         ((uint32_t)buf[1] << 16) |
         ((uint32_t)buf[2] << 8) |
         buf[3];
}

static int32_t peer_connection_read_s24_be(const uint8_t* buf) {
  int32_t value = ((int32_t)buf[0] << 16) |
                  ((int32_t)buf[1] << 8) |
                  buf[2];

  if (value & 0x800000) {
    value |= ~0xFFFFFF;
  }

  return value;
}

static void peer_connection_log_rtcp_rr(uint8_t* packet, size_t len) {
  uint8_t rc;
  uint32_t receiver_ssrc;

  if (packet == NULL || len < 8) {
    return;
  }

  rc = packet[0] & 0x1F;
  receiver_ssrc = peer_connection_read_u32_be(packet + 4);
  LOGD("RTCP_RR rc=%u receiver_ssrc=%" PRIu32, rc, receiver_ssrc);

  for (uint8_t i = 0; i < rc; i++) {
    size_t offset = 8 + 24 * i;

    if (len < offset + 24) {
      LOGW("RTCP_RR truncated report block: index=%u len=%zu", i, len);
      break;
    }

    LOGD("RTCP_RR block[%u] media_ssrc=%" PRIu32
         " fraction_lost=%u cumulative_lost=%" PRId32
         " highest_seq=%" PRIu32 " jitter=%" PRIu32
         " lsr=%" PRIu32 " dlsr=%" PRIu32,
         i,
         peer_connection_read_u32_be(packet + offset),
         packet[offset + 4],
         peer_connection_read_s24_be(packet + offset + 5),
         peer_connection_read_u32_be(packet + offset + 8),
         peer_connection_read_u32_be(packet + offset + 12),
         peer_connection_read_u32_be(packet + offset + 16),
         peer_connection_read_u32_be(packet + offset + 20));
  }
}

static void peer_connection_incoming_rtcp(PeerConnection* pc, uint8_t* buf, size_t len) {
  RtcpHeader* rtcp_header;
  size_t pos = 0;
  size_t packet_len;

  while (pos < len) {
    rtcp_header = (RtcpHeader*)(buf + pos);
    packet_len = 4 * ntohs(rtcp_header->length) + 4;

    switch (rtcp_header->type) {
      case RTCP_RR:
        peer_connection_log_rtcp_rr(buf + pos, packet_len);
        if (rtcp_header->rc > 0) {
          // TODO: Parse RR report blocks for sender-side QoS handling:
          // packet loss callback, jitter/RTT estimation, REMB, GCC, etc.
#if 0
          RtcpRr rtcp_rr = rtcp_parse_rr(buf);
          uint32_t fraction = ntohl(rtcp_rr.report_block[0].flcnpl) >> 24;
          uint32_t total = ntohl(rtcp_rr.report_block[0].flcnpl) & 0x00FFFFFF;
          if(pc->on_receiver_packet_loss && fraction > 0) {

            pc->on_receiver_packet_loss((float)fraction/256.0, total, pc->config.user_data);
          }
#endif
        }
        break;
      case RTCP_PSFB: {
        int fmt = rtcp_header->rc;
        LOGD("RTCP_PSFB fmt=%d%s", fmt, fmt == 1 ? " PLI" : (fmt == 4 ? " FIR" : ""));
        // PLI and FIR
        if ((fmt == 1 || fmt == 4) && pc->config.on_request_keyframe) {
          pc->config.on_request_keyframe(pc->config.user_data);
        }
      } break;
      case RTCP_BYE:
        LOGI("Got RTCP BYE");
        pc->rtcp_bye_sent = 1;
        if (pc->oniceconnectionstatechange && pc->state != PEER_CONNECTION_CLOSED) {
          pc->oniceconnectionstatechange(PEER_CONNECTION_CLOSED, pc->config.user_data);
        }
        pc->state = PEER_CONNECTION_CLOSED;
        break;
      default:
        break;
    }

    pos += packet_len;
  }
}

const char* peer_connection_state_to_string(PeerConnectionState state) {
  switch (state) {
    case PEER_CONNECTION_NEW:
      return "new";
    case PEER_CONNECTION_CHECKING:
      return "checking";
    case PEER_CONNECTION_CONNECTED:
      return "connected";
    case PEER_CONNECTION_COMPLETED:
      return "completed";
    case PEER_CONNECTION_FAILED:
      return "failed";
    case PEER_CONNECTION_CLOSED:
      return "closed";
    case PEER_CONNECTION_DISCONNECTED:
      return "disconnected";
    default:
      return "unknown";
  }
}

PeerConnectionState peer_connection_get_state(PeerConnection* pc) {
  return pc->state;
}

void* peer_connection_get_sctp(PeerConnection* pc) {
  return &pc->sctp;
}

static uint32_t peer_connection_generate_ssrc(uint32_t avoid_ssrc) {
  uint32_t ssrc = 0;

  do {
    ssrc = ports_random_u32();
  } while (ssrc == 0 || ssrc == avoid_ssrc);

  return ssrc;
}

static void peer_connection_init_local_ssrc(PeerConnection* pc) {
  pc->local_assrc = pc->config.local_audio_ssrc;
  pc->local_vssrc = pc->config.local_video_ssrc;

  if (pc->config.audio_codec != CODEC_NONE && pc->local_assrc == 0) {
    pc->local_assrc = peer_connection_generate_ssrc(0);
  }

  if (pc->config.video_codec != CODEC_NONE && (pc->local_vssrc == 0 || pc->local_vssrc == pc->local_assrc)) {
    pc->local_vssrc = peer_connection_generate_ssrc(pc->local_assrc);
  }
}

PeerConnection* peer_connection_create(PeerConfiguration* config) {
  PeerConnection* pc = calloc(1, sizeof(PeerConnection));
  if (!pc) {
    return NULL;
  }

  memcpy(&pc->config, config, sizeof(PeerConfiguration));
  peer_connection_init_local_ssrc(pc);

  agent_create(&pc->agent);

  memset(&pc->sctp, 0, sizeof(pc->sctp));

  if (pc->config.audio_codec) {
    if (pc->config.sdp_profile != SDP_PROFILE_WHEP) {
      rtp_encoder_init(&pc->artp_encoder, pc->config.audio_codec,
                       peer_connection_outgoing_rtp_packet, (void*)pc);
      pc->artp_encoder.ssrc = pc->local_assrc;
    }

    if (pc->config.sdp_profile != SDP_PROFILE_WHIP) {
      rtp_decoder_init(&pc->artp_decoder, pc->config.audio_codec,
                       pc->config.onaudiotrack, pc->config.user_data);
    }
  }

  if (pc->config.video_codec) {
    if (pc->config.sdp_profile != SDP_PROFILE_WHEP) {
      rtp_encoder_init(&pc->vrtp_encoder, pc->config.video_codec,
                       peer_connection_outgoing_rtp_packet, (void*)pc);
      pc->vrtp_encoder.ssrc = pc->local_vssrc;
    }

    if (pc->config.sdp_profile != SDP_PROFILE_WHIP) {
      rtp_decoder_init(&pc->vrtp_decoder, pc->config.video_codec,
                       pc->config.onvideotrack, pc->config.user_data);
    }
  }

  return pc;
}

void peer_connection_destroy(PeerConnection* pc) {
  if (pc) {
    peer_connection_send_rtcp_bye(pc);
    sctp_destroy_association(&pc->sctp);
    dtls_srtp_deinit(&pc->dtls_srtp);
    agent_destroy(&pc->agent);
    free(pc);
    pc = NULL;
  }
}

void peer_connection_close(PeerConnection* pc) {
  peer_connection_send_rtcp_bye(pc);
  pc->state = PEER_CONNECTION_CLOSED;
}

int peer_connection_send_audio(PeerConnection* pc, const uint8_t* buf, size_t len) {
  if (pc->state != PEER_CONNECTION_COMPLETED) {
    return -1;
  }
  if (pc->config.audio_codec == CODEC_NONE) {
    return -1;
  }
  return rtp_encoder_encode(&pc->artp_encoder, buf, len);
}

int peer_connection_send_video(PeerConnection* pc, const uint8_t* buf, size_t len) {
  if (pc->state != PEER_CONNECTION_COMPLETED) {
    return -1;
  }
  if (pc->config.video_codec == CODEC_NONE) {
    return -1;
  }
  return rtp_encoder_encode(&pc->vrtp_encoder, buf, len);
}

int peer_connection_datachannel_send(PeerConnection* pc, char* message, size_t len) {
  return peer_connection_datachannel_send_sid(pc, message, len, 0);
}

int peer_connection_datachannel_send_sid(PeerConnection* pc, char* message, size_t len, uint16_t sid) {
  if (!sctp_is_connected(&pc->sctp)) {
    LOGE("sctp not connected");
    return -1;
  }
  if (pc->config.datachannel == DATA_CHANNEL_STRING)
    return sctp_outgoing_data(&pc->sctp, message, len, PPID_STRING, sid);
  else
    return sctp_outgoing_data(&pc->sctp, message, len, PPID_BINARY, sid);
}

int peer_connection_create_datachannel(PeerConnection* pc, DecpChannelType channel_type, uint16_t priority, uint32_t reliability_parameter, char* label, char* protocol) {
  return peer_connection_create_datachannel_sid(pc, channel_type, priority, reliability_parameter, label, protocol, 0);
}

int peer_connection_create_datachannel_sid(PeerConnection* pc, DecpChannelType channel_type, uint16_t priority, uint32_t reliability_parameter, char* label, char* protocol, uint16_t sid) {
  int rtrn = -1;

  if (!sctp_is_connected(&pc->sctp)) {
    LOGE("sctp not connected");
    return rtrn;
  }

  //  0                   1                   2                   3
  //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // |  Message Type |  Channel Type |            Priority           |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // |                    Reliability Parameter                      |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // |         Label Length          |       Protocol Length         |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // |                                                               |
  // |                             Label                             |
  // |                                                               |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  // |                                                               |
  // |                            Protocol                           |
  // |                                                               |
  // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
  int msg_size = 12 + strlen(label) + strlen(protocol);
  uint16_t priority_big_endian = htons(priority);
  uint32_t reliability_big_endian = ntohl(reliability_parameter);
  uint16_t label_length = htons(strlen(label));
  uint16_t protocol_length = htons(strlen(protocol));
  char* msg = calloc(1, msg_size);
  if (!msg) {
    return rtrn;
  }

  msg[0] = DATA_CHANNEL_OPEN;
  memcpy(msg + 2, &priority_big_endian, sizeof(uint16_t));
  memcpy(msg + 4, &reliability_big_endian, sizeof(uint32_t));
  memcpy(msg + 8, &label_length, sizeof(uint16_t));
  memcpy(msg + 10, &protocol_length, sizeof(uint16_t));
  memcpy(msg + 12, label, strlen(label));
  memcpy(msg + 12 + strlen(label), protocol, strlen(protocol));

  rtrn = sctp_outgoing_data(&pc->sctp, msg, msg_size, PPID_CONTROL, sid);
  free(msg);
  return rtrn;
}

static DtlsSrtpRole peer_connection_answer_dtls_role(PeerConnection* pc) {
  switch (pc->remote_dtls_setup) {
    case REMOTE_DTLS_SETUP_ACTIVE:
      /* Remote offer setup:active -> remote initiates DTLS; we answer passive. */
      return DTLS_SRTP_ROLE_SERVER;
    case REMOTE_DTLS_SETUP_PASSIVE:
      /* Remote offer setup:passive -> we must initiate DTLS. */
      return DTLS_SRTP_ROLE_CLIENT;
    case REMOTE_DTLS_SETUP_ACTPASS:
    default:
      return DTLS_SRTP_ROLE_CLIENT;
  }
}

static const char* peer_connection_dtls_role_setup_value(SdpType sdp_type, DtlsSrtpRole d) {
  if (sdp_type == SDP_TYPE_OFFER) {
    return d == DTLS_SRTP_ROLE_CLIENT ? "a=setup:active" : "a=setup:actpass";
  }
  return d == DTLS_SRTP_ROLE_SERVER ? "a=setup:passive" : "a=setup:active";
}

int peer_connection_loop(PeerConnection* pc) {
  uint32_t ssrc = 0;
  memset(pc->agent_buf, 0, sizeof(pc->agent_buf));
  pc->agent_ret = -1;

  switch (pc->state) {
    case PEER_CONNECTION_NEW:
      break;

    case PEER_CONNECTION_CHECKING:
      if (pc->agent.candidate_pairs_num == 0) {
        /* Trickle ICE: remote candidates may arrive after local answer/offer. */
        break;
      }
      if (pc->config.skip_stun_check_keepalive) {
        if (agent_skip_connectivity_check(&pc->agent) == 0) {
          STATE_CHANGED(pc, PEER_CONNECTION_CONNECTED);
        } else {
          STATE_CHANGED(pc, PEER_CONNECTION_FAILED);
        }
      } else if (agent_select_candidate_pair(&pc->agent) < 0) {
        STATE_CHANGED(pc, PEER_CONNECTION_FAILED);
      } else if (agent_connectivity_check(&pc->agent) == 0) {
        STATE_CHANGED(pc, PEER_CONNECTION_CONNECTED);
      }
      break;

    case PEER_CONNECTION_CONNECTED: {
      Address* remote_addr = NULL;
      int hs_ret;

      if (pc->agent.selected_pair && pc->agent.selected_pair->remote) {
        remote_addr = &pc->agent.selected_pair->remote->addr;
      }

      pc->dtls_srtp.udp_send = peer_connection_dtls_srtp_send;
      pc->dtls_srtp.udp_recv = peer_connection_dtls_srtp_recv;

      /* Drain a few pending STUN packets; do not loop forever on keepalives. */
      agent_drain_pending(&pc->agent);

      {
        uint32_t now = ports_get_epoch_time();
        if (pc->dtls_hs_want_count > 0 && pc->last_dtls_hs_ms != 0 &&
            (now - pc->last_dtls_hs_ms) < 50) {
          break;
        }
        pc->last_dtls_hs_ms = now;
      }

      hs_ret = dtls_srtp_handshake_step(&pc->dtls_srtp, remote_addr);

      if (hs_ret == 0) {
        pc->dtls_hs_want_count = 0;
        peer_connection_dtls_rx_clear(pc);
        LOGI("DTLS-SRTP handshake done");

        if (pc->config.datachannel) {
          LOGI("SCTP create socket");
          sctp_create_association(&pc->sctp, &pc->dtls_srtp);
          pc->sctp.userdata = pc->config.user_data;
        }

        STATE_CHANGED(pc, PEER_CONNECTION_COMPLETED);
      } else if (peer_connection_dtls_handshake_retriable(hs_ret)) {
        pc->dtls_hs_want_count++;
        if (pc->dtls_hs_want_count == 1) {
          LOGI("DTLS handshake in progress (role=%s)",
               pc->dtls_srtp.role == DTLS_SRTP_ROLE_SERVER ? "server/passive" : "client/active");
        } else if (hs_ret != MBEDTLS_ERR_SSL_WANT_READ && hs_ret != MBEDTLS_ERR_SSL_WANT_WRITE &&
                   pc->dtls_hs_want_count <= 5) {
          LOGW("DTLS handshake waiting for more records (ret=-0x%.4x, count=%d)",
               (unsigned int)-hs_ret, pc->dtls_hs_want_count);
        } else if (pc->dtls_hs_want_count % 200 == 0) {
          LOGW("DTLS handshake still waiting (want=%d, count=%d)",
               hs_ret, pc->dtls_hs_want_count);
        }
        if (pc->dtls_hs_want_count > 500) {
          LOGE("DTLS handshake timeout after ICE connected");
          peer_connection_dtls_rx_clear(pc);
          dtls_srtp_reset_session(&pc->dtls_srtp);
          STATE_CHANGED(pc, PEER_CONNECTION_FAILED);
        }
      } else {
        pc->dtls_hs_want_count = 0;
        peer_connection_dtls_rx_clear(pc);
        dtls_srtp_reset_session(&pc->dtls_srtp);
        STATE_CHANGED(pc, PEER_CONNECTION_FAILED);
      }
    } break;
    case PEER_CONNECTION_COMPLETED:
      peer_connection_maybe_send_rtcp_rr(pc);

      if ((pc->agent_ret = agent_recv(&pc->agent, pc->agent_buf, sizeof(pc->agent_buf))) > 0) {
        LOGD("agent_recv %d", pc->agent_ret);

        if (rtcp_probe(pc->agent_buf, pc->agent_ret)) {
          LOGD("Got RTCP packet");
          dtls_srtp_decrypt_rtcp_packet(&pc->dtls_srtp, pc->agent_buf, &pc->agent_ret);
          peer_connection_incoming_rtcp(pc, pc->agent_buf, pc->agent_ret);

        } else if (peer_connection_datagram_is_dtls(pc->agent_buf, pc->agent_ret)) {
          peer_connection_handle_dtls_record(pc);

        } else if (rtp_packet_validate(pc->agent_buf, pc->agent_ret)) {
          LOGD("Got RTP packet");

          dtls_srtp_decrypt_rtp_packet(&pc->dtls_srtp, pc->agent_buf, &pc->agent_ret);

          ssrc = rtp_get_ssrc(pc->agent_buf);
          if (ssrc == pc->remote_assrc) {
            rtp_decoder_decode(&pc->artp_decoder, pc->agent_buf, pc->agent_ret);
          } else if (ssrc == pc->remote_vssrc) {
            rtp_decoder_decode(&pc->vrtp_decoder, pc->agent_buf, pc->agent_ret);
          }

        } else {
          LOGW("Unknown data");
        }
      }

      if (!pc->config.skip_stun_check_keepalive &&
          CONFIG_KEEPALIVE_TIMEOUT > 0 &&
          (ports_get_epoch_time() - pc->agent.binding_request_time) > CONFIG_KEEPALIVE_TIMEOUT) {
        LOGI("binding request timeout");
        STATE_CHANGED(pc, PEER_CONNECTION_CLOSED);
      }

      break;
    case PEER_CONNECTION_FAILED:
      break;
    case PEER_CONNECTION_DISCONNECTED:
      break;
    case PEER_CONNECTION_CLOSED:
      break;
    default:
      break;
  }

  return 0;
}

static void peer_connection_copy_fingerprint(char* dst, size_t dst_len, const char* line) {
  const char* fp = strstr(line, "sha-256");
  if (!fp) {
    fp = strstr(line, "SHA-256");
  }
  if (!fp) {
    return;
  }

  fp = strchr(fp, ' ');
  if (!fp) {
    return;
  }

  while (*fp == ' ') {
    fp++;
  }

  strncpy(dst, fp, dst_len - 1);
  dst[dst_len - 1] = '\0';
}

static int sdp_mline_first_payload_type(const char* line) {
  const char* pos = strstr(line, "SAVPF ");
  if (!pos) {
    pos = strstr(line, "SAVP ");
  }
  if (!pos) {
    return -1;
  }
  return atoi(pos + 6);
}

static void peer_connection_apply_answer_codec(PeerConnection* pc, const char* media,
                                               int payload_type, const char* rtpmap_line) {
  int pt = payload_type;
  int clock_rate = 0;
  char codec_name[32] = {0};

  if (rtpmap_line && sscanf(rtpmap_line, "a=rtpmap:%d %31[^/]/%d", &pt, codec_name, &clock_rate) >= 2) {
    if (clock_rate == 0) {
      clock_rate = 8000;
    }
  } else if (pt < 0) {
    return;
  } else {
    clock_rate = 8000;
  }

  if (strcmp(media, "audio") == 0 && pc->config.audio_codec != CODEC_NONE) {
    if (pc->config.sdp_profile != SDP_PROFILE_WHEP) {
      rtp_encoder_set_payload(&pc->artp_encoder, (uint8_t)pt, (uint32_t)clock_rate);
    }
    if (pc->config.sdp_profile != SDP_PROFILE_WHIP) {
      rtp_decoder_set_payload(&pc->artp_decoder, (uint8_t)pt);
    }
    LOGI("Negotiated audio codec %s PT=%d clock=%d", codec_name[0] ? codec_name : "PCMA", pt, clock_rate);
  } else if (strcmp(media, "video") == 0 && pc->config.video_codec != CODEC_NONE) {
    if (pc->config.sdp_profile != SDP_PROFILE_WHEP) {
      pc->vrtp_encoder.type = (uint8_t)pt;
      if (clock_rate > 0) {
        pc->vrtp_encoder.timestamp_increment = clock_rate / 30;
      }
    }
    if (pc->config.sdp_profile != SDP_PROFILE_WHIP) {
      rtp_decoder_set_payload(&pc->vrtp_decoder, (uint8_t)pt);
    }
    LOGI("Negotiated video codec %s PT=%d clock=%d", codec_name[0] ? codec_name : "H264", pt, clock_rate);
  }
}

void peer_connection_set_remote_description(PeerConnection* pc, const char* sdp, SdpType type) {
  char* start = (char*)sdp;
  char* line = NULL;
  char buf[256];
  char* val_start = NULL;
  uint32_t* ssrc = NULL;
  int has_setup = 0;
  DtlsSrtpRole role = DTLS_SRTP_ROLE_SERVER;
  int is_update = 0;
  Agent* agent = &pc->agent;
  const char* current_media = NULL;

  while ((line = strstr(start, "\r\n"))) {
    line = strstr(start, "\r\n");
    strncpy(buf, start, line - start);
    buf[line - start] = '\0';

    if (strstr(buf, "a=setup:actpass")) {
      role = DTLS_SRTP_ROLE_SERVER;
      has_setup = 1;
      if (type == SDP_TYPE_OFFER) {
        pc->remote_dtls_setup = REMOTE_DTLS_SETUP_ACTPASS;
      }
    } else if (strstr(buf, "a=setup:passive")) {
      role = DTLS_SRTP_ROLE_CLIENT;
      has_setup = 1;
      if (type == SDP_TYPE_OFFER) {
        pc->remote_dtls_setup = REMOTE_DTLS_SETUP_PASSIVE;
      }
    } else if (strstr(buf, "a=setup:active")) {
      role = DTLS_SRTP_ROLE_SERVER;
      has_setup = 1;
      if (type == SDP_TYPE_OFFER) {
        pc->remote_dtls_setup = REMOTE_DTLS_SETUP_ACTIVE;
      }
    }

    if (strstr(buf, "a=fingerprint")) {
      peer_connection_copy_fingerprint(pc->dtls_srtp.remote_fingerprint,
                                       sizeof(pc->dtls_srtp.remote_fingerprint),
                                       buf);
    }

    if (strstr(buf, "a=ice-ufrag") &&
        strlen(agent->remote_ufrag) != 0 &&
        (strncmp(buf + strlen("a=ice-ufrag:"), agent->remote_ufrag, strlen(agent->remote_ufrag)) == 0)) {
      is_update = 1;
    }

    if (strncmp(buf, "m=audio", 7) == 0) {
      current_media = "audio";
      ssrc = &pc->remote_assrc;
      if (type == SDP_TYPE_ANSWER) {
        peer_connection_apply_answer_codec(pc, "audio", sdp_mline_first_payload_type(buf), NULL);
      }
    } else if (strncmp(buf, "m=video", 7) == 0) {
      current_media = "video";
      ssrc = &pc->remote_vssrc;
      if (type == SDP_TYPE_ANSWER) {
        peer_connection_apply_answer_codec(pc, "video", sdp_mline_first_payload_type(buf), NULL);
      }
    } else if (strncmp(buf, "m=", 2) == 0) {
      current_media = NULL;
      ssrc = NULL;
    }

    if (type == SDP_TYPE_ANSWER && current_media && strncmp(buf, "a=rtpmap:", 9) == 0) {
      peer_connection_apply_answer_codec(pc, current_media, -1, buf);
    }

    if ((val_start = strstr(buf, "a=ssrc:")) && ssrc) {
      *ssrc = strtoul(val_start + 7, NULL, 10);
      LOGD("SSRC: %" PRIu32, *ssrc);
    }

    start = line + 2;
  }

  if (is_update) {
    return;
  }

  // Reconfigure DTLS role based on remote's setup value
  if (has_setup && type == SDP_TYPE_ANSWER) {
    if (pc->talk_dtls_client_offer) {
      role = DTLS_SRTP_ROLE_CLIENT;
      LOGI("Talk caller: keep DTLS client (offer was setup:active)");
    }
    dtls_srtp_reconfig(&pc->dtls_srtp, role);
    peer_connection_dtls_rx_clear(pc);
  }

  pc->dtls_handshake_started = 0;

  agent_set_remote_description(&pc->agent, (char*)sdp);
  if (type == SDP_TYPE_ANSWER) {
    agent_update_candidate_pairs(&pc->agent);
    STATE_CHANGED(pc, PEER_CONNECTION_CHECKING);
  }
}

static const char* peer_connection_create_sdp(PeerConnection* pc, SdpType sdp_type) {
  char* description = (char*)pc->temp_buf;

  memset(pc->temp_buf, 0, sizeof(pc->temp_buf));
  DtlsSrtpRole role = DTLS_SRTP_ROLE_SERVER;

  pc->sctp.connected = 0;
  pc->dtls_hs_want_count = 0;
  peer_connection_dtls_rx_clear(pc);

  switch (sdp_type) {
    case SDP_TYPE_OFFER:
      if (pc->config.sdp_profile == SDP_PROFILE_ZLM_TALK) {
        /* Talk caller: initiate DTLS (same as callee answer path). Avoids parsing
         * browser's fragmented ClientHello when we would otherwise be DTLS server. */
        role = DTLS_SRTP_ROLE_CLIENT;
        pc->talk_dtls_client_offer = 1;
      } else {
        role = DTLS_SRTP_ROLE_SERVER;
        pc->talk_dtls_client_offer = 0;
      }
      agent_clear_candidates(&pc->agent);
      pc->agent.mode = AGENT_MODE_CONTROLLING;
      pc->remote_dtls_setup = REMOTE_DTLS_SETUP_UNKNOWN;
      if (pc->config.sdp_profile == SDP_PROFILE_ZLM_TALK) {
        LOGI("Talk offer DTLS role: active/client (setup:active)");
      }
      break;
    case SDP_TYPE_ANSWER:
      pc->talk_dtls_client_offer = 0;
      role = peer_connection_answer_dtls_role(pc);
      pc->agent.mode = AGENT_MODE_CONTROLLED;
      LOGI("Answer DTLS role: %s (remote offer setup: %s)",
           role == DTLS_SRTP_ROLE_SERVER ? "passive/server" : "active/client",
           pc->remote_dtls_setup == REMOTE_DTLS_SETUP_ACTIVE ? "active" :
           pc->remote_dtls_setup == REMOTE_DTLS_SETUP_PASSIVE ? "passive" :
           pc->remote_dtls_setup == REMOTE_DTLS_SETUP_ACTPASS ? "actpass" : "unknown");
      break;
    default:
      break;
  }

  dtls_srtp_reset_session(&pc->dtls_srtp);
  dtls_srtp_init(&pc->dtls_srtp, role, pc);
  pc->dtls_srtp.udp_recv = peer_connection_dtls_srtp_recv;
  pc->dtls_srtp.udp_send = peer_connection_dtls_srtp_send;

  memset(pc->sdp, 0, sizeof(pc->sdp));
  // TODO: check if we have video or audio codecs
  sdp_create(pc->sdp,
             pc->config.video_codec != CODEC_NONE,
             pc->config.audio_codec != CODEC_NONE,
             pc->config.datachannel,
             pc->config.sdp_profile);

  agent_create_ice_credential(&pc->agent);
  sdp_append(pc->sdp, "a=ice-ufrag:%s", pc->agent.local_ufrag);
  sdp_append(pc->sdp, "a=ice-pwd:%s", pc->agent.local_upwd);
  sdp_append(pc->sdp, "a=fingerprint:sha-256 %s", pc->dtls_srtp.local_fingerprint);
  sdp_append(pc->sdp, peer_connection_dtls_role_setup_value(sdp_type, role));

  if (pc->config.video_codec == CODEC_H264) {
    sdp_append_h264(pc->sdp, pc->config.sdp_profile, pc->local_vssrc);
  }

  switch (pc->config.audio_codec) {
    case CODEC_PCMA:
      sdp_append_pcma(pc->sdp, pc->config.sdp_profile, pc->local_assrc);
      break;
    case CODEC_PCMU:
      sdp_append_pcmu(pc->sdp, pc->config.sdp_profile, pc->local_assrc);
      break;
    case CODEC_OPUS:
      sdp_append_opus(pc->sdp, pc->config.sdp_profile, pc->local_assrc);
      break;
    default:
      break;
  }

  if (pc->config.datachannel) {
    sdp_append_datachannel(pc->sdp, pc->config.sdp_profile);
  }

  pc->b_local_description_created = 1;

  agent_gather_candidate(&pc->agent, NULL, NULL, NULL);  // host address
  for (int i = 0; i < sizeof(pc->config.ice_servers) / sizeof(pc->config.ice_servers[0]); ++i) {
    if (pc->config.ice_servers[i].urls) {
      LOGI("ice server: %s", pc->config.ice_servers[i].urls);
      agent_gather_candidate(&pc->agent, pc->config.ice_servers[i].urls, pc->config.ice_servers[i].username, pc->config.ice_servers[i].credential);
    }
  }

  agent_get_local_description(&pc->agent, description, sizeof(pc->temp_buf));
  sdp_append(pc->sdp, description);

  if (pc->onicecandidate) {
    pc->onicecandidate(pc->sdp, pc->config.user_data);
  }

  return pc->sdp;
}

const char* peer_connection_create_offer(PeerConnection* pc) {
  return peer_connection_create_sdp(pc, SDP_TYPE_OFFER);
}

const char* peer_connection_create_answer(PeerConnection* pc) {
  const char* sdp = peer_connection_create_sdp(pc, SDP_TYPE_ANSWER);
  agent_update_candidate_pairs(&pc->agent);
  STATE_CHANGED(pc, PEER_CONNECTION_CHECKING);
  return sdp;
}

int peer_connection_send_rtcp_pil(PeerConnection* pc, uint32_t ssrc) {
  int ret = -1;
  uint8_t plibuf[128];
  rtcp_get_pli(plibuf, 12, ssrc);

  // TODO: encrypt rtcp packet
  // guint size = 12;
  // dtls_transport_encrypt_rctp_packet(pc->dtls_transport, plibuf, &size);
  // ret = nice_agent_send(pc->nice_agent, pc->stream_id, pc->component_id, size, (gchar*)plibuf);

  return ret;
}

// callbacks
void peer_connection_on_connected(PeerConnection* pc, void (*on_connected)(void* userdata)) {
  pc->on_connected = on_connected;
}

void peer_connection_on_receiver_packet_loss(PeerConnection* pc,
                                             void (*on_receiver_packet_loss)(float fraction_loss, uint32_t total_loss, void* userdata)) {
  pc->on_receiver_packet_loss = on_receiver_packet_loss;
}

void peer_connection_onicecandidate(PeerConnection* pc, void (*onicecandidate)(char* sdp, void* userdata)) {
  pc->onicecandidate = onicecandidate;
}

void peer_connection_oniceconnectionstatechange(PeerConnection* pc,
                                                void (*oniceconnectionstatechange)(PeerConnectionState state, void* userdata)) {
  pc->oniceconnectionstatechange = oniceconnectionstatechange;
}

void peer_connection_ondatachannel(PeerConnection* pc,
                                   void (*onmessage)(char* msg, size_t len, void* userdata, uint16_t sid),
                                   void (*onopen)(void* userdata),
                                   void (*onclose)(void* userdata)) {
  if (pc) {
    sctp_onopen(&pc->sctp, onopen);
    sctp_onclose(&pc->sctp, onclose);
    sctp_onmessage(&pc->sctp, onmessage);
  }
}

int peer_connection_lookup_sid(PeerConnection* pc, const char* label, uint16_t* sid) {
  for (int i = 0; i < pc->sctp.stream_count; i++) {
    if (strncmp(pc->sctp.stream_table[i].label, label, sizeof(pc->sctp.stream_table[i].label)) == 0) {
      *sid = pc->sctp.stream_table[i].sid;
      return 0;
    }
  }
  return -1;  // Not found
}

char* peer_connection_lookup_sid_label(PeerConnection* pc, uint16_t sid) {
  for (int i = 0; i < pc->sctp.stream_count; i++) {
    if (pc->sctp.stream_table[i].sid == sid) {
      return pc->sctp.stream_table[i].label;
    }
  }
  return NULL;  // Not found
}

int peer_connection_add_ice_candidate(PeerConnection* pc, char* candidate) {
  Agent* agent = &pc->agent;
  if (ice_candidate_from_description(&agent->remote_candidates[agent->remote_candidates_count], candidate, candidate + strlen(candidate)) != 0) {
    return -1;
  }
  LOGD("Add candidate: %s", candidate);
  agent->remote_candidates_count++;
  agent_update_candidate_pairs(agent);

  if (pc->state == PEER_CONNECTION_FAILED && agent->candidate_pairs_num > 0) {
    STATE_CHANGED(pc, PEER_CONNECTION_CHECKING);
  }
  return 0;
}
