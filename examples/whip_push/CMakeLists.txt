project(whip_push)

file(GLOB SRCS "*.c")

include_directories(${CMAKE_SOURCE_DIR}/src)

add_executable(whip_push ${SRCS})

target_link_libraries(whip_push peer pthread)
