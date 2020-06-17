cmake_minimum_required(VERSION 3.2 FATAL_ERROR)

get_filename_component(SAMPLE_PROJECT "${CMAKE_CURRENT_SOURCE_DIR}" NAME)

project(${SAMPLE_PROJECT} LANGUAGES C CXX)

find_library(KAFKA_LIBRARY rdkafka++)
find_package(rxpp CONFIG)

# define the sources
set(SAMPLE_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/main.cpp
)

MESSAGE( STATUS "using clang settings" )
add_compile_options( -Wall -Wextra -Werror )
add_compile_options( -std=c++11 -stdlib=libc++ )

add_executable(${SAMPLE_PROJECT} ${SAMPLE_SOURCES})
#target_compile_options(${SAMPLE_PROJECT} PUBLIC ${RX_COMPILE_OPTIONS})
#target_compile_features(${SAMPLE_PROJECT} PUBLIC ${RX_COMPILE_FEATURES})
#target_include_directories(${SAMPLE_PROJECT} PUBLIC ${RXCPP_LIBRARY})
include_directories("/usr/local/include")
target_link_libraries(${SAMPLE_PROJECT} ${CMAKE_THREAD_LIBS_INIT} ${KAFKA_LIBRARY})
