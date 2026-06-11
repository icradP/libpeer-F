project(whep_pull)

file(GLOB SRCS "*.c")

include_directories(${CMAKE_SOURCE_DIR}/src)

add_executable(whep_pull ${SRCS})

target_link_libraries(whep_pull peer pthread)
