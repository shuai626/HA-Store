
add_library(txn STATIC
    txn/storage.cc
    txn/mvcc_storage.cc
    txn/txn.cc
    txn/txn_processor.cc
    txn/lock_manager.cc
)
target_link_libraries(txn PUBLIC Threads::Threads)

add_executable(lock_manager_test
    txn/lock_manager_test.cc
)
target_link_libraries(lock_manager_test PUBLIC txn)

add_executable(txn_processor_test
    txn/txn_processor_test.cc
)
target_link_libraries(txn_processor_test PUBLIC txn)

add_executable(txn_types_test
    txn/txn_types_test.cc
)
target_link_libraries(txn_types_test PUBLIC txn)

add_test(NAME lock_manager_test COMMAND lock_manager_test)
add_test(NAME txn_processor_test COMMAND txn_processor_test)
add_test(NAME txn_types_test COMMAND txn_types_test)
