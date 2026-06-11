project(datachannel)

file(GLOB SRCS "*.c")

include_directories(${CMAKE_SOURCE_DIR}/src)

add_executable(datachannel ${SRCS})

target_link_libraries(datachannel peer pthread)
