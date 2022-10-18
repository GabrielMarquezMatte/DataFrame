add_library(DataFrame STATIC DataFrame.hpp DataFrame.cpp)
target_link_libraries(DataFrame PUBLIC ${Boost_LIBRARIES})
target_include_directories(DataFrame PUBLIC ${Boost_INCLUDE_DIRS})