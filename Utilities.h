#include "string"
#include "iostream"
#include "fstream"
#include <string.h>

/**
 *  class for defining utility functions to be used
 *  across the project. Import "utility.h" to use it.
**/
class Utilities {
    
    public:
    
        // check if file exists already at the given file path
        static int isFileExists (const std::string& name) {
            if (FILE *file = fopen(name.c_str(), "r")) { fclose(file); return 1; }  
            else { return 0; }   
        }

        static int getUniqueCounter () {
            ifstream ifile;
            ofstream ofile;
            int counter = 0;
            if (isFileExists("counter.txt")){
                ifile.open("counter.txt",ios::in);
                ifile.read((char*)&counter,sizeof(int));
                ifile.close();
            }
            counter++;
            ofile.open("counter.txt",ios::out);
            ofile.write((char*)&counter,sizeof(int));
            ofile.close();
            return counter;
        }   

        static char* newTempFile(char* extension) {
            if ((extension == NULL) || (extension[0] == '\0')) { extension=""; }
            string str("temp_" + to_string(Utilities::getUniqueCounter()) + extension);
            char *cstr = new char[str.length() + 1];
            strcpy(cstr, str.c_str());
            return cstr;
        }
};
