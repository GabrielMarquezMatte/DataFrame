add_library(DataFrame STATIC class_header.hpp DataFrame.cpp constructors.hpp DataFrame.hpp load_data.hpp df_operations.hpp other_functions.hpp)
target_link_libraries(DataFrame PUBLIC ${Boost_LIBRARIES} xlnt::xlnt)
target_include_directories(DataFrame PUBLIC ${Boost_INCLUDE_DIRS})