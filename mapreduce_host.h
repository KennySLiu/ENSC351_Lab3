#ifndef ENSC351_MAPREDUCE_HOST
#define ENSC351_MAPREDUCE_HOST

#include "kenny_include.h"
#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <pthread.h>
#include <utility>
#include <algorithm>
#include <vector>


struct kv_pair{
    std::string key;
    int value;
};



template<class keytype, class valuetype>
bool mapvect_sort_func(std::pair<keytype, valuetype> a, std::pair<keytype, valuetype> b){
    return (a.first < b.first);
}

template<class keytype, class valuetype>
void mapreduce(std::vector<keytype> input_reader (void*), std::pair<keytype, valuetype> map_func (keytype), std::pair<keytype, valuetype> reduce_func (std::vector<std::pair<keytype, valuetype> >), void* output_func (std::vector<std::pair<keytype, valuetype> >), void* file){
    using namespace std;
    
    /* INPUT READER: */
    vector<keytype> parsed_input = input_reader(file);
    /* END OF INPUT READER SECTION */


    /* MAPPER: */
    vector<pair<keytype, valuetype> > mapped_kv_pairs;
    for (int i = 0; i < parsed_input.size(); ++i){
        mapped_kv_pairs.push_back(map_func(parsed_input[i]));
    }   
    /* END OF MAPPER SECTION */



    /* Sort the vector: */
    sort(mapped_kv_pairs.begin(), mapped_kv_pairs.end(), mapvect_sort_func<keytype, valuetype>);

    

    /* REDUCER: */
    vector<pair<keytype, valuetype> > reduced_kv_pairs;

    for (int i = 0; i < mapped_kv_pairs.size(); ++i){
        vector<pair<keytype, valuetype> > mapped_kv_pairs_subVect;
    
        // Run until the j'th key is not equal to the i'th key:
        for (int j = i; j < mapped_kv_pairs.size() && static_cast<keytype>(mapped_kv_pairs[j].first) == static_cast<keytype>(mapped_kv_pairs[i].first); ++j){
            // Copy the elements that match one another
            pair<keytype, valuetype> cur = mapped_kv_pairs[j];
            mapped_kv_pairs_subVect.push_back(cur);
            i = j;
        }

        // Pass in the subVector to reduced_kv_pairs.
        reduced_kv_pairs.push_back(reduce_func(mapped_kv_pairs_subVect));
    }
    /* END OF REDUCER SECTION */



    /* Debug: 
    for (int i = 0; i < reduced_kv_pairs.size(); ++i){
        pair<keytype, valuetype> curpair = reduced_kv_pairs[i];

        keytype curkey = static_cast<keytype>(curpair.first);
        valuetype curval = static_cast<valuetype>(curpair.second);

        cout << curkey << ", " << curval << "\n";
    }
    */

    /* OUTPUTTER: */
    output_func(reduced_kv_pairs);
    /* END OF OUTPUTTER SECTION */
    


}




#endif
