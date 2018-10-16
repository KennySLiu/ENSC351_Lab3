#include "mapreduce_host.h"
#include "kenny_include.h"
#include <iostream>
#include <fstream>
#include <time.h>
#include <stdlib.h>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <algorithm>
#include <utility>
#include <assert.h>
#define MAP_NUM_THREADS 4
#define INFILE "WORDCOUNT_INPUT_FILE"
#define OUTFILE "WORDCOUNT_OUTPUT_FILE"

using namespace std;
//bool map_func_comparator(pair<string, int> a, pair<string, int> b);


vector<string> input_reader(void* file){
    /*  Inputs: 
            "file" must be a file pointer so we can read inputs
        Outputs:
            An array of key-value pairs.
    */
    
    string filename = *(static_cast<string*>(file));
    ifstream in_file;
    in_file.open(filename);
    string cur_line;

    vector<string> retval;

    if (in_file.is_open()){

        while (getline(in_file, cur_line)){
            /* This part plays around with cstrings and std::strings, because
                 * tokenize only works on cstrings. */
            char cstr_cur_line[cur_line.size() + 1];
            char* cstr_tok;

            // Copy the current line into a C-string, so we can call strtok
            strcpy(cstr_cur_line, &cur_line[0]);
            // Grab the first word of the line (if it exists)
            cstr_tok = strtok(cstr_cur_line, " \n");
            while (cstr_tok != NULL){
                // Convert it into a string so we can pack it into the string_counts vector
                string tok_p = cstr_tok;
                retval.push_back(tok_p);
                // Continue tokenizing from where we last stopped.
                cstr_tok = strtok(NULL, " \n");
            } 
        }
    } else {
        cout << "\nINPUT_READER: The file didn't open correctly oh god oh man" << endl;
    }
    
    
    return retval;
}

pair<string, int> map_func(string input){
    /* INPUTS: 
     *      "input" must be a pointer to a string.
     * OUTPUTS:
     *       returns a pair of pointers. The first pointer is a string, the 2nd pointer is an int = 1.
     */
    int counter = 1;
    
    return make_pair(input, counter);
}
    

pair<string, int> reduce_func(vector<pair<string, int> > input){
    /* INPUTS:
     *      "vp_input" must be a vector of pair<void*, void*> that can be
     *          converted into pair<string, int> (like in map_func(), above).
     *          The keys (string portion) must be the same for each call.
     * 
     * OUTPUTS:
     *      returns a pair of pointers. The first pointer is a string, the 2nd pointer is an int = sum of all input ints.
     */

    // Grab the input that vp_input actually points at
    pair<string, int> first_elem = input[0];
    string reduce_word = first_elem.first;
    int count = 0;

    for (int i = 0; i < input.size(); ++i){
        pair<string, int> cur_elem = input[i];
        string cur_string = cur_elem.first;
    
        // If the word in the current element is NOT the word that we are currently reducing, abort. Something went wrong.
        assert( cur_string == reduce_word );
        ++count;
    }
    
    
    return make_pair(reduce_word, count);
}
        


void* output_func(vector<pair<string, int> > input){
    string filename = OUTFILE;
    ofstream outfile;
    outfile.open(filename);
    
    for (int i = 0; i < input.size(); ++i){
        pair<string, int> cur = input[i];
        outfile << cur.first << ": " << cur.second << endl;
    }
    return NULL;
}



int main(){
    long double start_time = get_time();

    string* filename = new string(INFILE);
    void* file = static_cast<void*>(filename);
    mapreduce<string, int>(*input_reader, *map_func, *reduce_func, *output_func, file);

    long double finish_time = get_time();

    cout << finish_time - start_time << endl;

    return 0;
}







// THIS IS OLD STUFF THAT IS PROBABLY WRONG
// THIS IS OLD STUFF THAT IS PROBABLY WRONG
// THIS IS OLD STUFF THAT IS PROBABLY WRONG
// THIS IS OLD STUFF THAT IS PROBABLY WRONG
// THIS IS OLD STUFF THAT IS PROBABLY WRONG
/*
void* OLD_map_func(void* vp_string_vect){
    //  Inputs:
     *      vp_string_vect = void pointer to the vector of tokenized strings.
     *  Outputs:
     *      This program returns a pair<void*, void*> ret
     *      ret.second is a sorted vector of pair<string, int>s, 
     *        sorted alphabetically on the strings.
     *      ret.first is an array of indices that delimit different 
     *        strings in ret.second.
     /
    
    // Cast the void pointer to a normal vector of strings.
    vector<string>* string_vect = static_cast<vector<string>*>(vp_string_vect);
    vector<pair<string, int> > * string_counts;

    for (size_t i = 0; i < string_vect->size(); i += MAP_NUM_THREADS){
        string_counts->push_back(make_pair((*string_vect)[i], 1));
    }

    // Since we probably won't be sorting the vector in the host, we'll do that here. /
    sort( (*string_counts).begin(), (*string_counts).end(), map_func_comparator);

    // The host also needs to know how many threads to spawn, so we'll give a vector of indices that 
     * delimit groups of different key values. For example, from v[1] to v[2]-1 we have "cat". / 
    pair<void*, void*>* ret = new pair<void*, void*>;
    ret->second = static_cast<void*>(string_counts);
    vector<int>* indexmap = new vector<int>;
    vector<string> seen;
    int index = 0;
    
    // The vector of strings will already be sorted by this point. /
    for (size_t i = 0; i < string_counts->size(); ++i){
        if (find(seen.begin(), seen.end(), (*string_counts)[i].first) != seen.end()){
            // If the current string is not in seen /
            (*indexmap)[index] = i;
            ++index;
            seen.push_back((*string_counts)[i].first);
        }
    }
    ret->first = static_cast<void*>(indexmap);

    return static_cast<void*>(ret);

}

bool OLD_map_func_comparator( pair<string, int> a, pair<string, int> b ){
    string astring = a.first;
    string bstring = b.first;
    int res = astring.compare(bstring);
    if (res == -1){
        res = 0;
    } else {
        res = 1;
    }
    return (bool) res;
}
*/
