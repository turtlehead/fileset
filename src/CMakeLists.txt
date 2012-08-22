add_subdirectory(unrar)
include_directories("${PROJECT_SOURCE_DIR}/src/unrar")

add_definitions(-D_UNIX)
add_executable(fileset archive.c load_dat.c main.c miniz.c traverse.c utils.c)
target_link_libraries(fileset UnRar ${SQLITE3_LIBRARY} ${MHASH_LIBRARY})

install(TARGETS fileset DESTINATION bin)
