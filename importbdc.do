* Import DTA File
* Created: [Current Date]
* Last Modified: [Current Date]

* Clear any existing data in memory
clear all

* Set working directory (change this to your actual directory path)
cd "C:\Your\Directory\Path"

* Import the .dta file (replace "filename.dta" with your actual file name)
use "filename.dta", clear

* Display a summary of the imported data
describe

* Show the first few observations
list in 1/10

* Save the imported data (optional)
* save "newfilename.dta", replace

* End of do-file