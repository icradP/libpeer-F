#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include "rtcp.h"

#define NTP_UNIX_EPOCH_OFFSET 2208988800ULL

static void rtcp_write_u32_be(uint8_t* packet, int offset, uint32_t value) {
  packet[offset] = (uint8_t)(value >> 24);
  packet[offset + 1] = (uint8_t)(value >> 16);
  packet[offset + 2] = (uint8_t)(value >> 8);
  packet[offset + 3] = (uint8_t)(value);
}

static void rtcp_get_ntp_timestamp(uint32_t* ntp_sec, uint32_t* ntp_frac) {
  struct timeval tv;

  gettimeofday(&tv, NULL);
  *ntp_sec = (uint32_t)((uint64_t)tv.tv_sec + NTP_UNIX_EPOCH_OFFSET);
  *ntp_frac = (uint32_t)(((uint64_t)tv.tv_usec << 32) / 1000000ULL);
}

int rtcp_probe(uint8_t* packet, size_t size) {
  if (size < 8)
    return 0;

  if ((packet[0] & 0xC0) != 0x80)
    return 0;

  uint8_t pt = packet[1];
  // RFC 5761 mux range, or standard RTCP packet types.
  return (pt >= 64 && pt <= 95) || (pt >= 192 && pt <= 223);
}

int rtcp_build_sr(uint8_t* packet, int len, uint32_t ssrc, uint32_t rtp_timestamp, uint32_t packet_count, uint32_t octet_count) {
  if (packet == NULL || len < 28)
    return -1;

  memset(packet, 0, len);
  packet[0] = 0x80;
  packet[1] = RTCP_SR;
  packet[2] = 0;
  packet[3] = 6;

  uint32_t ntp_sec;
  uint32_t ntp_frac;

  rtcp_get_ntp_timestamp(&ntp_sec, &ntp_frac);

  rtcp_write_u32_be(packet, 4, ssrc);
  rtcp_write_u32_be(packet, 8, ntp_sec);
  rtcp_write_u32_be(packet, 12, ntp_frac);
  rtcp_write_u32_be(packet, 16, rtp_timestamp);
  rtcp_write_u32_be(packet, 20, packet_count);
  rtcp_write_u32_be(packet, 24, octet_count);

  return 28;
}

int rtcp_build_rr(uint8_t* packet, int len, uint32_t receiver_ssrc, uint32_t media_ssrc) {
  if (packet == NULL || len < 32 || media_ssrc == 0)
    return -1;

  // TODO: Fill RR report block statistics for WHEP QoS feedback:
  // fraction lost, cumulative lost, extended highest sequence, jitter,
  // LSR, and DLSR. This minimal RR is currently used as RTCP keepalive.
  memset(packet, 0, len);
  packet[0] = 0x81;  // V=2, RC=1
  packet[1] = RTCP_RR;
  packet[2] = 0;
  packet[3] = 7;     // 32 bytes / 4 - 1

  rtcp_write_u32_be(packet, 4, receiver_ssrc);
  rtcp_write_u32_be(packet, 8, media_ssrc);

  return 32;
}

int rtcp_build_bye(uint8_t* packet, int len, uint32_t ssrc) {
  if (packet == NULL || len < 8 || ssrc == 0)
    return -1;

  memset(packet, 0, len);
  packet[0] = 0x81;  // V=2, SC=1
  packet[1] = RTCP_BYE;
  packet[2] = 0;
  packet[3] = 1;     // 8 bytes / 4 - 1

  rtcp_write_u32_be(packet, 4, ssrc);

  return 8;
}

int rtcp_get_pli(uint8_t* packet, int len, uint32_t ssrc) {
  if (packet == NULL || len != 12)
    return -1;

  memset(packet, 0, len);
  packet[0] = 0x80;
  packet[1] = RTCP_PSFB;
  packet[2] = 0;
  packet[3] = (len / 4) - 1;
  packet[4] = (uint8_t)(ssrc >> 24);
  packet[5] = (uint8_t)(ssrc >> 16);
  packet[6] = (uint8_t)(ssrc >> 8);
  packet[7] = (uint8_t)(ssrc);

  return 12;
}

int rtcp_get_fir(uint8_t* packet, int len, int* seqnr) {
  if (packet == NULL || len != 20 || seqnr == NULL)
    return -1;

  memset(packet, 0, len);
  *seqnr = *seqnr + 1;
  if (*seqnr < 0 || *seqnr >= 256)
    *seqnr = 0;

  packet[0] = 0x81;
  packet[1] = RTCP_PSFB;
  packet[2] = 0;
  packet[3] = (len / 4) - 1;
  packet[16] = (uint8_t)(*seqnr);

  return 20;
}

RtcpRr rtcp_parse_rr(uint8_t* packet) {
  RtcpRr rtcp_rr;
  memcpy(&rtcp_rr.header, packet, sizeof(rtcp_rr.header));
  memcpy(&rtcp_rr.report_block[0], packet + 8, 6 * sizeof(uint32_t));

  return rtcp_rr;
}
