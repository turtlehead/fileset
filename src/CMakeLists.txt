add_executable(fileset main.c)
target_link_libraries(fileset ${SQLITE3_LIBRARY} ${MHASH_LIBRARY} ${ZIP_LIBRARY})
