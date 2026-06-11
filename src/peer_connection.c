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

#define STATE_CHANGED(pc, curr_state)                                 \
  if (pc->oniceconnectionstatechange && pc->state != curr_state) {    \
    pc->oniceconnectionstatechange(curr_state, pc->config.user_data); \
    pc->state = curr_state;                                           \
  }

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

  uint32_t local_vpackets;
  uint32_t local_voctets;
  uint32_t local_apackets;
  uint32_t local_aoctets;
  uint32_t last_rtcp_sr_ms;
  int dtls_handshake_started;
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

static int peer_connection_dtls_srtp_recv(void* ctx, unsigned char* buf, size_t len) {
  int recv_max = 0;
  int ret = -1;
  DtlsSrtp* dtls_srtp = (DtlsSrtp*)ctx;
  PeerConnection* pc = (PeerConnection*)dtls_srtp->user_data;

  if (pc->agent_ret > 0 && pc->agent_ret <= len) {
    memcpy(buf, pc->agent_buf, pc->agent_ret);
    ret = pc->agent_ret;
    pc->agent_ret = 0;
    return ret;
  }

  while (recv_max < CONFIG_TLS_READ_TIMEOUT && pc->state == PEER_CONNECTION_CONNECTED) {
    ret = agent_recv_datagram(&pc->agent, buf, len);

    if (ret > 0) {
      if (peer_connection_datagram_is_dtls((const uint8_t*)buf, ret)) {
        return ret;
      }
      continue;
    }

    recv_max++;
  }
  return ret;
}

static int peer_connection_dtls_srtp_send(void* ctx, const uint8_t* buf, size_t len) {
  DtlsSrtp* dtls_srtp = (DtlsSrtp*)ctx;
  PeerConnection* pc = (PeerConnection*)dtls_srtp->user_data;

  // LOGD("send %.4x %.4x, %ld", *(uint16_t*)buf, *(uint16_t*)(buf + 2), len);
  return agent_send(&pc->agent, buf, len);
}

static void peer_connection_incoming_rtcp(PeerConnection* pc, uint8_t* buf, size_t len) {
  RtcpHeader* rtcp_header;
  size_t pos = 0;

  while (pos < len) {
    rtcp_header = (RtcpHeader*)(buf + pos);

    switch (rtcp_header->type) {
      case RTCP_RR:
        LOGD("RTCP_PR");
        if (rtcp_header->rc > 0) {
// TODO: REMB, GCC ...etc
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
        LOGD("RTCP_PSFB %d", fmt);
        // PLI and FIR
        if ((fmt == 1 || fmt == 4) && pc->config.on_request_keyframe) {
          pc->config.on_request_keyframe(pc->config.user_data);
        }
      }
      default:
        break;
    }

    pos += 4 * ntohs(rtcp_header->length) + 4;
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

PeerConnection* peer_connection_create(PeerConfiguration* config) {
  PeerConnection* pc = calloc(1, sizeof(PeerConnection));
  if (!pc) {
    return NULL;
  }

  memcpy(&pc->config, config, sizeof(PeerConfiguration));

  agent_create(&pc->agent);

  memset(&pc->sctp, 0, sizeof(pc->sctp));

  if (pc->config.audio_codec) {
    if (pc->config.sdp_profile != SDP_PROFILE_WHEP) {
      rtp_encoder_init(&pc->artp_encoder, pc->config.audio_codec,
                       peer_connection_outgoing_rtp_packet, (void*)pc);
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
    sctp_destroy_association(&pc->sctp);
    dtls_srtp_deinit(&pc->dtls_srtp);
    agent_destroy(&pc->agent);
    free(pc);
    pc = NULL;
  }
}

void peer_connection_close(PeerConnection* pc) {
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

static const char* peer_connection_dtls_role_setup_value(SdpType sdp_type, DtlsSrtpRole d) {
  if (sdp_type == SDP_TYPE_OFFER) {
    return "a=setup:actpass";
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
      if (pc->agent.selected_pair && pc->agent.selected_pair->remote) {
        remote_addr = &pc->agent.selected_pair->remote->addr;
      }
      pc->dtls_srtp.udp_send = peer_connection_dtls_srtp_send;
      pc->dtls_srtp.udp_recv = peer_connection_dtls_srtp_recv;

      if (pc->dtls_handshake_started) {
        break;
      }
      pc->dtls_handshake_started = 1;

      agent_drain_pending(&pc->agent);

      if (dtls_srtp_handshake(&pc->dtls_srtp, remote_addr) == 0) {
        LOGD("DTLS-SRTP handshake done");

        if (pc->config.datachannel) {
          LOGI("SCTP create socket");
          sctp_create_association(&pc->sctp, &pc->dtls_srtp);
          pc->sctp.userdata = pc->config.user_data;
        }

        STATE_CHANGED(pc, PEER_CONNECTION_COMPLETED);
      } else {
        dtls_srtp_reset_session(&pc->dtls_srtp);
        STATE_CHANGED(pc, PEER_CONNECTION_FAILED);
      }
      } break;
    case PEER_CONNECTION_COMPLETED:
      if ((pc->agent_ret = agent_recv(&pc->agent, pc->agent_buf, sizeof(pc->agent_buf))) > 0) {
        LOGD("agent_recv %d", pc->agent_ret);

        if (rtcp_probe(pc->agent_buf, pc->agent_ret)) {
          LOGD("Got RTCP packet");
          dtls_srtp_decrypt_rtcp_packet(&pc->dtls_srtp, pc->agent_buf, &pc->agent_ret);
          peer_connection_incoming_rtcp(pc, pc->agent_buf, pc->agent_ret);

        } else if (dtls_srtp_probe(pc->agent_buf)) {
          int ret = dtls_srtp_read(&pc->dtls_srtp, pc->temp_buf, sizeof(pc->temp_buf));
          LOGD("Got DTLS data %d", ret);

          if (ret > 0) {
            sctp_incoming_data(&pc->sctp, (char*)pc->temp_buf, ret);
          }

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

    if (strstr(buf, "a=setup:active")) {
      // remote will initiate DTLS -> we are server (passive)
      role = DTLS_SRTP_ROLE_SERVER;
      has_setup = 1;
    } else if (strstr(buf, "a=setup:passive")) {
      // remote waits -> we are client (active, must initiate)
      role = DTLS_SRTP_ROLE_CLIENT;
      has_setup = 1;
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
    dtls_srtp_reconfig(&pc->dtls_srtp, role);
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

  switch (sdp_type) {
    case SDP_TYPE_OFFER:
      role = DTLS_SRTP_ROLE_SERVER;
      agent_clear_candidates(&pc->agent);
      pc->agent.mode = AGENT_MODE_CONTROLLING;
      break;
    case SDP_TYPE_ANSWER:
      role = DTLS_SRTP_ROLE_CLIENT;
      pc->agent.mode = AGENT_MODE_CONTROLLED;
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
    sdp_append_h264(pc->sdp, pc->config.sdp_profile);
  }

  switch (pc->config.audio_codec) {
    case CODEC_PCMA:
      sdp_append_pcma(pc->sdp, pc->config.sdp_profile);
      break;
    case CODEC_PCMU:
      sdp_append_pcmu(pc->sdp, pc->config.sdp_profile);
      break;
    case CODEC_OPUS:
      sdp_append_opus(pc->sdp, pc->config.sdp_profile);
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
  return 0;
}
