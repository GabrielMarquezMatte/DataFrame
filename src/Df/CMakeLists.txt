add_library(DataFrame STATIC class_header.hpp DataFrame.cpp constructors.hpp DataFrame.hpp load_data.hpp df_operations.hpp)
target_link_libraries(DataFrame PUBLIC ${Boost_LIBRARIES})
target_include_directories(DataFrame PUBLIC ${Boost_INCLUDE_DIRS})