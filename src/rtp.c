#include <stdio.h>
#include <string.h>

#include "address.h"
#include "config.h"
#include "peer_connection.h"
#include "rtp.h"
#include "utils.h"

typedef enum RtpH264Type {

  NALU = 23,
  FU_A = 28,

} RtpH264Type;

typedef struct NaluHeader {
  uint8_t type : 5;
  uint8_t nri : 2;
  uint8_t f : 1;
} NaluHeader;

typedef struct FuHeader {
  uint8_t type : 5;
  uint8_t r : 1;
  uint8_t e : 1;
  uint8_t s : 1;
} FuHeader;

#define RTP_PAYLOAD_SIZE (CONFIG_MTU - RTP_HEADER_SIZE)
#define FU_PAYLOAD_SIZE (CONFIG_MTU - RTP_HEADER_SIZE - sizeof(FuHeader) - sizeof(NaluHeader))

void rtp_header_write(uint8_t* packet, uint8_t pt, int marker, uint16_t seq, uint32_t timestamp, uint32_t ssrc) {
  packet[0] = 0x80;
  packet[1] = (marker ? 0x80 : 0) | (pt & 0x7F);
  packet[2] = (uint8_t)(seq >> 8);
  packet[3] = (uint8_t)(seq);
  packet[4] = (uint8_t)(timestamp >> 24);
  packet[5] = (uint8_t)(timestamp >> 16);
  packet[6] = (uint8_t)(timestamp >> 8);
  packet[7] = (uint8_t)(timestamp);
  packet[8] = (uint8_t)(ssrc >> 24);
  packet[9] = (uint8_t)(ssrc >> 16);
  packet[10] = (uint8_t)(ssrc >> 8);
  packet[11] = (uint8_t)(ssrc);
}

uint8_t rtp_header_payload_type(const uint8_t* packet) {
  return packet[1] & 0x7F;
}

int rtp_packet_validate(uint8_t* packet, size_t size) {
  if (size < RTP_HEADER_SIZE)
    return 0;

  if ((packet[0] & 0xC0) != 0x80)
    return 0;

  uint8_t pt = rtp_header_payload_type(packet);
  // RFC 5761: PT 64-95 on the RTP port are RTCP when using rtcp-mux.
  return pt < 64 || pt >= 96;
}

uint32_t rtp_get_ssrc(uint8_t* packet) {
  return ((uint32_t)packet[8] << 24) | ((uint32_t)packet[9] << 16) | ((uint32_t)packet[10] << 8) | packet[11];
}

int rtp_packet_header_size(const uint8_t* packet, size_t size) {
  if (size < RTP_HEADER_SIZE) {
    return -1;
  }

  int offset = RTP_HEADER_SIZE + 4 * (packet[0] & 0x0F);

  if (packet[0] & 0x10) {
    if (size < (size_t)offset + 4) {
      return -1;
    }
    int ext_len = ((packet[offset + 2] << 8) | packet[offset + 3]) * 4;
    offset += 4 + ext_len;
  }

  if (size < (size_t)offset) {
    return -1;
  }

  return offset;
}

static int rtp_encoder_encode_h264_single(RtpEncoder* rtp_encoder, uint8_t* buf, size_t size) {
  uint8_t* packet = rtp_encoder->buf;
  int marker = 0;

  if ((*buf & 0x1f) == 0x05 || (*buf & 0x1f) == 0x01) {
    marker = 1;
    rtp_encoder->timestamp += rtp_encoder->timestamp_increment;
  }

  rtp_header_write(packet, rtp_encoder->type, marker, rtp_encoder->seq_number++, rtp_encoder->timestamp, rtp_encoder->ssrc);

  memcpy(packet + RTP_HEADER_SIZE, buf, size);
  rtp_encoder->on_packet(packet, size + RTP_HEADER_SIZE, rtp_encoder->user_data);
  return 0;
}

static int rtp_encoder_encode_h264_fu_a(RtpEncoder* rtp_encoder, uint8_t* buf, size_t size) {
  uint8_t* packet = rtp_encoder->buf;
  uint8_t type = buf[0] & 0x1f;
  uint8_t nri = (buf[0] & 0x60) >> 5;
  buf = buf + 1;
  size = size - 1;

  if (type == 0x05 || type == 0x01) {
    rtp_encoder->timestamp += rtp_encoder->timestamp_increment;
  }

  NaluHeader* fu_indicator = (NaluHeader*)(packet + RTP_HEADER_SIZE);
  FuHeader* fu_header = (FuHeader*)(packet + RTP_HEADER_SIZE + sizeof(NaluHeader));
  fu_header->s = 1;

  while (size > 0) {
    fu_indicator->type = FU_A;
    fu_indicator->nri = nri;
    fu_indicator->f = 0;
    fu_header->type = type;
    fu_header->r = 0;

    if (size <= FU_PAYLOAD_SIZE) {
      fu_header->e = 1;
      rtp_header_write(packet, rtp_encoder->type, 1, rtp_encoder->seq_number++, rtp_encoder->timestamp, rtp_encoder->ssrc);
      memcpy(packet + RTP_HEADER_SIZE + sizeof(NaluHeader) + sizeof(FuHeader), buf, size);
      rtp_encoder->on_packet(packet, size + RTP_HEADER_SIZE + sizeof(NaluHeader) + sizeof(FuHeader), rtp_encoder->user_data);
      break;
    }

    fu_header->e = 0;
    rtp_header_write(packet, rtp_encoder->type, 0, rtp_encoder->seq_number++, rtp_encoder->timestamp, rtp_encoder->ssrc);
    memcpy(packet + RTP_HEADER_SIZE + sizeof(NaluHeader) + sizeof(FuHeader), buf, FU_PAYLOAD_SIZE);
    rtp_encoder->on_packet(packet, CONFIG_MTU, rtp_encoder->user_data);
    size -= FU_PAYLOAD_SIZE;
    buf += FU_PAYLOAD_SIZE;

    fu_header->s = 0;
  }
  return 0;
}

static uint8_t* h264_find_nalu(uint8_t* buf_start, uint8_t* buf_end) {
  uint8_t* p = buf_start + 2;

  while (p < buf_end) {
    if (*(p - 2) == 0x00 && *(p - 1) == 0x00 && *p == 0x01)
      return p + 1;
    p++;
  }

  return buf_end;
}

static int rtp_encoder_encode_h264(RtpEncoder* rtp_encoder, uint8_t* buf, size_t size) {
  uint8_t* buf_end = buf + size;
  uint8_t *pstart, *pend;
  size_t nalu_size;

  for (pstart = h264_find_nalu(buf, buf_end); pstart < buf_end; pstart = pend) {
    pend = h264_find_nalu(pstart, buf_end);
    nalu_size = pend - pstart;

    if (pend != buf_end)
      nalu_size--;

    while (pstart[nalu_size - 1] == 0x00)
      nalu_size--;

    if (nalu_size <= RTP_PAYLOAD_SIZE) {
      rtp_encoder_encode_h264_single(rtp_encoder, pstart, nalu_size);

    } else {
      rtp_encoder_encode_h264_fu_a(rtp_encoder, pstart, nalu_size);
    }
  }

  return 0;
}

static int rtp_encoder_encode_generic(RtpEncoder* rtp_encoder, uint8_t* buf, size_t size) {
  uint8_t* packet = rtp_encoder->buf;

  rtp_header_write(packet, rtp_encoder->type, 0, rtp_encoder->seq_number++, rtp_encoder->timestamp, rtp_encoder->ssrc);
  rtp_encoder->timestamp += rtp_encoder->timestamp_increment;
  memcpy(packet + RTP_HEADER_SIZE, buf, size);

  rtp_encoder->on_packet(packet, size + RTP_HEADER_SIZE, rtp_encoder->user_data);

  return 0;
}

void rtp_encoder_init(RtpEncoder* rtp_encoder, MediaCodec codec, RtpOnPacket on_packet, void* user_data) {
  rtp_encoder->on_packet = on_packet;
  rtp_encoder->user_data = user_data;
  rtp_encoder->timestamp = 0;
  rtp_encoder->seq_number = 0;

  switch (codec) {
    case CODEC_H264:
      rtp_encoder->type = PT_H264;
      rtp_encoder->ssrc = SSRC_H264;
      rtp_encoder->timestamp_increment = 90000 / 30;
      rtp_encoder->encode_func = rtp_encoder_encode_h264;
      break;
    case CODEC_PCMA:
      rtp_encoder->type = PT_PCMA;
      rtp_encoder->ssrc = SSRC_PCMA;
      rtp_encoder->timestamp_increment = CONFIG_AUDIO_DURATION * 8000 / 1000;
      rtp_encoder->encode_func = rtp_encoder_encode_generic;
      break;
    case CODEC_PCMU:
      rtp_encoder->type = PT_PCMU;
      rtp_encoder->ssrc = SSRC_PCMU;
      rtp_encoder->timestamp_increment = CONFIG_AUDIO_DURATION * 8000 / 1000;
      rtp_encoder->encode_func = rtp_encoder_encode_generic;
      break;
    case CODEC_OPUS:
      rtp_encoder->type = PT_OPUS;
      rtp_encoder->ssrc = SSRC_OPUS;
      rtp_encoder->timestamp_increment = CONFIG_AUDIO_DURATION * 48000 / 1000;
      rtp_encoder->encode_func = rtp_encoder_encode_generic;
      break;
    default:
      break;
  }
}

void rtp_encoder_set_payload(RtpEncoder* rtp_encoder, uint8_t payload_type, uint32_t clock_rate) {
  if (rtp_encoder == NULL || clock_rate == 0) {
    return;
  }

  rtp_encoder->type = payload_type;
  rtp_encoder->timestamp_increment = CONFIG_AUDIO_DURATION * clock_rate / 1000;
}

void rtp_decoder_set_payload(RtpDecoder* rtp_decoder, uint8_t payload_type) {
  if (rtp_decoder == NULL) {
    return;
  }

  rtp_decoder->type = payload_type;
}

int rtp_encoder_encode(RtpEncoder* rtp_encoder, const uint8_t* buf, size_t size) {
  return rtp_encoder->encode_func(rtp_encoder, (uint8_t*)buf, size);
}

static int rtp_decode_h264(RtpDecoder* rtp_decoder, uint8_t* buf, size_t size) {
  static const uint32_t nalu_start_4bytecode = 0x01000000;
  static uint8_t nalu_buf[CONFIG_MAX_NALU_SIZE];
  static int offset = 0;
  int header_size = rtp_packet_header_size(buf, size);
  if (header_size < 0) {
    return -1;
  }

  uint8_t* payload = buf + header_size;
  uint8_t nalu_type = *payload & 0x1f;
  int payload_size = (int)size - header_size;
  if (nalu_type > 0 && nalu_type < 24) {
    memcpy(nalu_buf, &nalu_start_4bytecode, sizeof(nalu_start_4bytecode));
    offset = sizeof(nalu_start_4bytecode);
    memcpy(nalu_buf + offset, payload, payload_size);
    offset += payload_size;
    if (rtp_decoder->on_packet != NULL) {
      rtp_decoder->on_packet(nalu_buf, offset, rtp_decoder->user_data);
    }
    return (int)size;
  } else {
    NaluHeader* fu_indicator = (NaluHeader*)payload;
    FuHeader* fu_header = (FuHeader*)(payload + sizeof(NaluHeader));
    uint8_t reconstructed_nalu_type = (fu_indicator->f << 7) | (fu_indicator->nri << 5) | fu_header->type;
    payload_size -= sizeof(NaluHeader) + sizeof(FuHeader);
    if (fu_header->s) {
      memcpy(nalu_buf, &nalu_start_4bytecode, sizeof(nalu_start_4bytecode));
      offset = sizeof(nalu_start_4bytecode);
      memcpy(nalu_buf + offset, &reconstructed_nalu_type, 1);
      offset += 1;
      memcpy(nalu_buf + offset, payload + 2, payload_size);
      offset += payload_size;
    } else if (offset < CONFIG_MAX_NALU_SIZE) {
      memcpy(nalu_buf + offset, payload + 2, payload_size);
      offset += payload_size;
      if (fu_header->e) {
        if (rtp_decoder->on_packet != NULL) {
          rtp_decoder->on_packet(nalu_buf, offset, rtp_decoder->user_data);
        }
        offset = 0;
      }
    }
  }
  return 0;
}

static int rtp_decode_generic(RtpDecoder* rtp_decoder, uint8_t* buf, size_t size) {
  int header_size = rtp_packet_header_size(buf, size);
  if (header_size < 0) {
    return -1;
  }

  if (rtp_decoder->on_packet != NULL) {
    rtp_decoder->on_packet(buf + header_size, size - (size_t)header_size, rtp_decoder->user_data);
  }
  return (int)size;
}

void rtp_decoder_init(RtpDecoder* rtp_decoder, MediaCodec codec, RtpOnPacket on_packet, void* user_data) {
  rtp_decoder->on_packet = on_packet;
  rtp_decoder->user_data = user_data;

  switch (codec) {
    case CODEC_H264:
      rtp_decoder->decode_func = rtp_decode_h264;
      break;
    case CODEC_PCMA:
    case CODEC_PCMU:
    case CODEC_OPUS:
      rtp_decoder->decode_func = rtp_decode_generic;
    default:
      break;
  }
}

int rtp_decoder_decode(RtpDecoder* rtp_decoder, const uint8_t* buf, size_t size) {
  if (rtp_decoder->decode_func == NULL)
    return -1;
  return rtp_decoder->decode_func(rtp_decoder, (uint8_t*)buf, size);
}
