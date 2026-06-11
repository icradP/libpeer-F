project(zlm_p2p)

file(GLOB SRCS "*.c")

include_directories(${CMAKE_SOURCE_DIR}/src)

add_executable(zlm_p2p ${SRCS})

target_link_libraries(zlm_p2p peer pthread)
