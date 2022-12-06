cmake_minimum_required(VERSION 3.20)
project(p2c)

find_package(PkgConfig REQUIRED)
find_package(Threads REQUIRED)

set(ENV{PKG_CONFIG_PATH} "/opt/bb/lib/pkgconfig:/usr/pgsql-14/lib/pkgconfig")

pkg_check_modules(CDB2API cdb2api REQUIRED)
#pkg_check_modules(PGTYPES libpgtypes REQUIRED)
pkg_check_modules(PQ libpq REQUIRED)
pkg_check_modules(UNWIND libunwind REQUIRED)


add_executable(p2c p2c.c)
target_include_directories(p2c PRIVATE ${CDB2API_INCLUDE_DIRS} ${PQ_INCLUDE_DIRS})
target_link_libraries(p2c ${CDB2API_LINK_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} /usr/pgsql-14/lib/libpgtypes.a ${PQ_LINK_LIBRARIES} ${UNWIND_LINK_LIBRARIES})
