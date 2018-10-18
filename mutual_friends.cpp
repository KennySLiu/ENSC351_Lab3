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
    unordered_map<string, int> friends;
    
    bool operator<(const person& rhs) const{
        if ( name < rhs.name)
            return true;
        else
            return false;
    }
    
    bool operator==(const person& rhs) const{
        if (name == rhs.name)
            return true;
        else 
            return false;
    }

};

vector< pair<person, person> > input_reader(void* file){
    
    string filename = *(static_cast<string*>(file));
    ifstream in_file;
    in_file.open(filename);
    string cur_line;

    vector<person> list_of_people;
    vector<pair<person, person> > retval;

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
                cur_person.friends[persons_friend] = 1;
            
                cstr_tok = strtok(NULL, " \n");
            }

            list_of_people.push_back( cur_person );
        }
    } else { 
        cout << "\nINPUT_READER: The file didn't open correctly oh noooooo" << endl;
    }

    pair<person, person> val_to_push;
    for (int i = 0; i < list_of_people.size(); ++i){
        person cur_person = list_of_people[i];
        unordered_map<string, int> cur_friends = cur_person.friends;

        for (unordered_map<string, int>::iterator friend_it = cur_friends.begin(); friend_it != cur_friends.end(); ++friend_it){
            // Make sure the input file wasn't malformed... (I can't be my own friend)
            assert(friend_it->first != cur_person.name);
            for (int j = 0; j < list_of_people.size(); ++j){
                if (list_of_people[j].name == friend_it->first){
                    val_to_push = make_pair(cur_person, list_of_people[j]);
                    break;
                }
            }
            retval.push_back(val_to_push);
        }
    }

    return retval;
}


pair< pair<person, person>, unordered_map<string, int> > map_func(pair<person, person> input){
    unordered_map<string, int> friends_of_p1 = input.first.friends;

    // This function has to sort the pair of people, so the reducer behaviour doesn't fail...
    if (input.first.name < input.second.name){
        person tmp = input.first;
        input.first = input.second;
        input.second = tmp;
    }
    return make_pair(input, friends_of_p1);
}

pair< pair<person, person>, unordered_map<string, int> > reduce_func(vector< pair < pair<person, person>, unordered_map<string, int> > > input){

    // For this program, the input to a reduce function must only contain two elements.
    // For example, (Bob, Jane) : (Jane, Miguel, Dave)
    // and          (Bob, Jane) : (Bob, Miguel, Shannon)
    assert(input.size() == 2);
    


    // first_elem is something of the form (Bob, Jane) : (Jane, Miguel, Dave)
    // and second_elem is something like   (Bob, Jane) : (Bob, Miguel, Shannon)
    pair< pair<person, person>, unordered_map<string, int> > first_elem = input[0];
    pair< pair<person, person>, unordered_map<string, int> > second_elem = input[1];
    pair<person, person> reduce_pair = input[0].first;
    assert(input[1].first == reduce_pair);

    // Get the friendlists for simpler semantics & less confusing code
    unordered_map<string, int>* first_friendlist = &(input[0].second);
    unordered_map<string, int>* second_friendlist = &(input[1].second);

    pair<person, person> ret_key = reduce_pair;
    unordered_map<string, int> reduced_value;

    for (unordered_map<string, int>::iterator buddy = first_friendlist->begin(); buddy != first_friendlist->end(); ++buddy){
        if ((*second_friendlist)[buddy->first] == 1){
            // If the buddy is friends with the first person AND the second person, we need to add them
            reduced_value[buddy->first] = 1;
        }   //otherwise they're not friends with both, so we don't add them to the mutual friends. 
    }
    return make_pair(ret_key, reduced_value);
}
            
void* output_func(vector< pair< pair<person, person> , unordered_map<string, int> > > input){
    string filename = OUTFILE;
    ofstream outfile;
    outfile.open(filename);

    for (int i = 0; i < input.size(); ++i){
        pair< pair<person, person> , unordered_map<string, int> >* cur = &input[i];
        unordered_map<string, int>* cur_friends = &(cur->second);
        
        outfile << "(" << cur->first.first.name << ", " << cur->first.second.name << ": ";
        
        for (unordered_map<string, int>::iterator buddy = cur_friends->begin(); buddy != cur_friends->end(); ++buddy){
            outfile << buddy->first << ", ";
        }
        outfile << endl;
    }
}


int main(){
    long double start_time = get_time();

    string* filename = new string(INFILE);
    void* file = static_cast<void*>(filename);
    mapreduce< pair<person, person> , unordered_map<string, int> >(*input_reader, *map_func, *reduce_func, *output_func, file);

    long double finish_time = get_time();

    cout << finish_time - start_time << endl;

    return 0;
}











