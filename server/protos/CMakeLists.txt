file(GLOB ProtoFiles "${CMAKE_CURRENT_SOURCE_DIR}/*.proto")
PROTOBUF_GENERATE_CPP(ProtoSources ProtoHeaders ${ProtoFiles})
add_library(messages STATIC ${ProtoSources} ${ProtoHeaders})
target_include_directories(messages
  PRIVATE ${PROTOBUF_INCLUDE_DIRS}
)
target_link_libraries(messages ${Boost_LIBRARIES} ${PROTOBUF_LIBRARY})
