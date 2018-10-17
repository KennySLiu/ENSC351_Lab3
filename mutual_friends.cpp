#include "mapreduce_host.h"
#include "kenny_include.h"
#include <iostream>
#include <fstream>
#include <time.h>
#include <stdlib.h>
#include <string>
#include <unordered_map>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <algorithm>
#include <utility>
#include <assert.h>
#define INFILE "MUTUALFRIENDS_INPUT_FILE"
#define OUTFILE "MUTUALFRIENDS_OUTPUT_FILE"

using namespace std;

struct person{
    string name;
    vector<string> friends;
}

vector<person> input_reader(void* file){
    
    string filename = *(static_cast<string*>(file));
    ifstream in_file;
    in_file.open(filename);
    string cur_line;

    vector<person> retval;

    if (in_file.is_open()){
        int person_count = 0;
    
        while (getline(in_file, cur_line)){
            person cur_person;

            char cstr_cur_line[cur_line.size() + 1];
            char* cstr_tok;

            strcpy(cstr_cur_line, &cur_line[0]);
        
            // The first name in the line will be the person's name
            cstr_tok = strtok(cstr_cur_line, " \n");
            cur_person.name = cstr_tok;
            
            // All subsequent names will be their friends
            cstr_tok = strtok(cstr_cur_line, " \n");
            while (cstr_tok != NULL){
                string persons_friend = cstr_tok;
                cur_person.friends.push_back( persons_friend );
            
                cstr_tok = stroktok(NULL, " \n");
            }

            retval.push_back( cur_person );
        }
    } else { 
        cout << "\nINPUT_READER: The file didn't open correctly oh noooooo" << endl;
    }
    return retval;
}


pair< pair<person, person>, unordered_map<vector<string>> > map_func(pair<person, person> input){
    unordered_map<vector<string> > friends_of_p1 = input.first.friends;

    return make_pair(input, friends_of_p1);
}

pair< pair<person, person>, vector<string> > reduce_func(vector< pair < pair<person, person>, vector<string> > > input){
    
    pair< pair<person, person>, vector<string> > first_elem = input[0];
    pair<person, person> reduce_pair = first_elem.first;
    vector<string> reduced_value;

    for (int i = 0; i < input.size(); ++i){
        pair< pair<person, person>, vector<string> > cur_elem = input[i];
        pair<person, person>* cur_pair = &cur_elem.first;
        assert( *cur_pair == reduce_pair );
        
        vector<string>* cur_friendslist = &cur_elem.second;

        for (int j = 0; j < cur_friendslist->size(); ++j){
            
        










                



