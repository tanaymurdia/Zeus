#ifndef ZEUS_CLIENT_FFI_H
#define ZEUS_CLIENT_FFI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FfiClient ZeusClient;

ZeusClient* zeus_client_create(uint64_t local_id);
void zeus_client_destroy(ZeusClient* client);
uint64_t zeus_client_local_id(const ZeusClient* client);
int32_t zeus_client_connect(ZeusClient* client, const char* address);
int32_t zeus_client_send_state(ZeusClient* client,
    float px, float py, float pz,
    float vx, float vy, float vz);
int32_t zeus_client_poll(ZeusClient* client,
    uint8_t* buffer, int32_t buffer_len);
int32_t zeus_parse_status(const uint8_t* data, int32_t len,
    uint16_t* entity_count, uint8_t* node_count,
    uint8_t* map_width, uint8_t* ball_radius);
int32_t zeus_parse_player_ids(const uint8_t* data, int32_t len,
    uint64_t* player_ids, int32_t max_players);
int32_t zeus_parse_state_update(const uint8_t* data, int32_t len,
    uint64_t* entity_ids, float* positions_xyz,
    float* velocities_xyz, int32_t max_entities);

#ifdef __cplusplus
}
#endif

#endif
