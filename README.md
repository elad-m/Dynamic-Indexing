# Dynamic-Indexing
An academic project about indexing big data where the data can change.  

To run the code:
1. Extract the E4.jar file on your machine (this will create a directory with the name ‘E4’):  
jar xf E4.jar
2. Enter the directory created:  
cd E4
3. Compile the source code:  
javac dynamic_index/*.java
5. Run:  
java -Xmx1G -Xms1G dynamic_index/Main

The project was done as part of a course titled 'Web Information Retreival' - aka index building aka search engines - as part of my bachelor's degree in Computer Science in The Hebrew University of Jerusalem.
In the course we learned about different aspects of search engines and I chose to test two different methods of building and keeping an index that changes contantly.

The source code purpose is mainly to show the process (in git's commits) in which the project was done. It was not done from scratch though, as it was built on the core of previous exercises in the course.

The 'E4.jar' purpose is to make it easy to run an (small) example. It contains the final source code and some input files. 

The 'How to run the code.pdf'  is straight forward. Above are the minimal steps for running the code, while in the pdf file you'll find more information about the running aspect of the project, and bigger data inputs (E5 and E6).

Finally, the 'Web Information Retrieval Project.pdf' is the the 'paper' I wrote and contains a more in-depth take on what this project does and why, results of the experiment and conclusions. Also there are beautiful  graphs here.
