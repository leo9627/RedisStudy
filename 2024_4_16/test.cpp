#include <iostream>
#include <fstream>
#include<cstdlib>
#include </usr/local/include/DataSketches/cpc_sketch.hpp>
#include </usr/local/include/DataSketches/cpc_union.hpp>

//simplified file operations and no error handling for clarity
int main(int argc, char **argv) {
  const int lg_k = 10;

  // this section generates two sketches with some overlap and serializes them into files
  {
    // 100000 distinct keys
    datasketches::cpc_sketch sketch1(lg_k);
    for (int key = 1; key <=100; key++) 
    {
      char s[1000];
      sprintf(s,"%d",key);
      sketch1.update(s);
    }
    
    std::ofstream os1("cpc_sketch1.bin");
    sketch1.serialize(os1);
    std::cout << "Distinct count estimate12: " << sketch1.get_estimate() << std::endl;
    std::cout<<"haha"<<std::endl;

    
    // 100000 distinct keys
    datasketches::cpc_sketch sketch2(lg_k);
    for (int key = 50000; key < 150000; key++) sketch2.update(key);
    std::ofstream os2("cpc_sketch2.bin");
    sketch2.serialize(os2);
  }

  // this section deserializes the sketches, produces union and prints the result
  {
    std::ifstream is1("cpc_sketch1.bin");
    auto sketch1 = datasketches::cpc_sketch::deserialize(is1);

    std::ifstream is2("cpc_sketch2.bin");
    auto sketch2 = datasketches::cpc_sketch::deserialize(is2);

    datasketches::cpc_union u(lg_k);
    u.update(sketch1);
    u.update(sketch2);
    auto sketch = u.get_result();

    // debug summary of the union result sketch
    std::cout<<sketch.to_string();

    std::cout << "Distinct count estimate: " << sketch.get_estimate() << std::endl;
    std::cout << "Distinct count lower bound 95% confidence: " << sketch.get_lower_bound(2) << std::endl;
    std::cout << "Distinct count upper bound 95% confidence: " << sketch.get_upper_bound(2) << std::endl;
  }

  return 0;
}