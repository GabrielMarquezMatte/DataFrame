#include "Df/DataFrame.hpp"
bool SelectTest()
{
    try
    {
        df::data_map<int, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int, std::string> df(data);
        assert (df.getCols() == 2);
        assert (df.getRows() == 10);
        df::DataFrame<int, std::string> df2 = df.select(df::column_set{"id"});
        assert (df2.getCols() == 1);
        assert (df2.getRows() == 10);
        return true;
    }
    catch(std::exception& e)
    {
        return false;
    }
}
bool ConcatTest()
{
    try
    {
        df::data_map<int, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int, std::string> df(data);
        assert (df.getRows() == 10);
        df::data_map<int, std::string> data2 = {
            {"id", {11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
            {"age", {110, 120, 130, 140, 150, 160, 170, 180, 190, 200}}};
        df::DataFrame<int, std::string> df2(data2);
        assert (df2.getRows() == 10);
        df.concat(df2);
        assert (df.getRows() == 20);
        return true;
    }
    catch(std::exception& e)
    {
        return false;
    }
}
bool GetRowsAndColsTest()
{
    try
    {
        df::data_map<int, std::string> data = {
            {"id", {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
            {"age", {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}}};
        df::DataFrame<int, std::string> df(data);
        assert (df.getRows() == 10);
        assert (df.getCols() == 2);
        return true;
    }
    catch(std::exception& e)
    {
        return false;
    }
}
int main()
{
    if(SelectTest())
        std::cout << "Test 1 passed\n";
    else
        std::cout << "Test 1 failed\n";
    if(ConcatTest())
        std::cout << "Test 2 passed\n";
    else
        std::cout << "Test 2 failed\n";
    if(GetRowsAndColsTest())
        std::cout << "Test 3 passed\n";
    else
        std::cout << "Test 3 failed\n";
    return 0;
}