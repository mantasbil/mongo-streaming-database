# Music streaming database using MongoDB

This project presents a very simplified sample database of a music streaming service using MongoDB. It contains information about artist and their albums. The schema of database is presented in the figure below
![db-schema](https://github.com/user-attachments/assets/e21794db-8989-4c37-906c-c827c2674a35)

File Artists.py contains a few structured sample entries of artists in the database.
File Albums.py contains a few structured sample entries of albums in the database.

main.py file contains methods of inserting new data into database, clearing the database, displaying all the collections in the database, displaying everything about the albums as well as calculating album duration based on durations of its tracks using two different methods - 1) aggregate and 2) map-reduce.
