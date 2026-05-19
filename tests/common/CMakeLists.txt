include(GoogleTest)

enable_testing()
link_directories(${POSTGRES_LIB_DIR})

# ==================== FalconCMIT =================
add_executable(FalconCMIT
    ${PROJECT_SOURCE_DIR}/tests/common/test_falcon_cm.cpp
    ${common_src}
)
target_link_libraries(FalconCMIT
    FalconStore
    FalconClient
    zookeeper_mt
    glog
    jsoncpp
    gtest
    pq
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)

gtest_discover_tests(FalconCMIT)

# ==================== FalconStatsIOUT =================
add_executable(FalconStatsIOUT
    ${PROJECT_SOURCE_DIR}/tests/common/test_falcon_stats_io.cpp
    ${common_src}
)
target_link_libraries(FalconStatsIOUT
    FalconStore
    FalconClient
    zookeeper_mt
    glog
    jsoncpp
    gtest
    pq
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)

gtest_discover_tests(FalconStatsIOUT)

# ==================== IORecordAggregatorUT =================
add_executable(IORecordAggregatorUT
    ${PROJECT_SOURCE_DIR}/tests/common/test_io_record_aggregator.cpp
    ${common_src}
)
target_link_libraries(IORecordAggregatorUT
    FalconStore
    FalconClient
    zookeeper_mt
    glog
    jsoncpp
    gtest
    pq
    ${BRPC_LIBRARIES}
    ${DYNAMIC_LIB}
)

gtest_discover_tests(IORecordAggregatorUT)
