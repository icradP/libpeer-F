#ifndef SDP_H_
#define SDP_H_

#include <stdint.h>
#include <string.h>
#include "config.h"

#define SDP_ATTR_LENGTH 128

#ifndef ICE_LITE
#define ICE_LITE 0
#endif

typedef enum SdpProfile {
  SDP_PROFILE_P2P = 0,
  SDP_PROFILE_WHIP,
  SDP_PROFILE_WHEP,
} SdpProfile;

void sdp_append_h264(char* sdp, SdpProfile profile, uint32_t ssrc);

void sdp_append_pcma(char* sdp, SdpProfile profile, uint32_t ssrc);

void sdp_append_pcmu(char* sdp, SdpProfile profile, uint32_t ssrc);

void sdp_append_opus(char* sdp, SdpProfile profile, uint32_t ssrc);

void sdp_append_datachannel(char* sdp, SdpProfile profile);

void sdp_create(char* sdp, int b_video, int b_audio, int b_datachannel, SdpProfile profile);

int sdp_append(char* sdp, const char* format, ...);

void sdp_reset(char* sdp);

#endif  // SDP_H_
