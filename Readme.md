## TPSQL (Task Parallel SQL)
### Asynchronous tasks for parallely executing SQL query in technoloically distributed data warehouse.

Steps to use build this project:
- The project already contains a customized version of Northwind database. Inside the folder `/src/db` you can find folders `/localdbX` which represents remote local data warehouses. The tables within localdbX represents region-wise local data.
- This project uses `make` for build automation. Please make sure you have `make` installed already in your system.
  
    For Windows: install chocolatey and then run `choco install make`

    For Ubuntu 18.04 and above: Run `sudo apt install make`

1. Download or clone this repository. [Download](https://github.com/AshirwadPradhan/tpsql/archive/master.zip)
2. Extract and nagivate to `/tpsql`.
3. To prepare the environment run `make prepare`. This will install all the dependencies to run this project.
4. To run the servers:
   As there are 6 regional databases we wish to run 6 multiple servers. Open up 6 terminals and run `make run-dw PORT=500x` in each of them. Replace `x` with `0 ... 5`. Here we are runnning 6 servers on ports `5000 5001 5002 5003 5004 5005`.
5. To run the aggregator:
   Open up another terminal window and run `make run-agg`.
6. Now run the client using `make run-client`.
7. Now we can execute some queries in the client.
8. Some sample queries include:
   1. `select a.CategoryName from Categories a inner join Products b on a.CategoryID = b.CategoryID where b.Discontinued='N' order by b.ProductName`
   2. `select OrderID,sum(UnitPrice*Quantity*(1-Discount)) as Subtotal from order_details group by OrderID`
   3. `select productID,productName,categoryID from products`
9. Observations:
    Navigate to `/src/tmp` and we can find the partial output of all the taskmanagers. The number of task managers that ran until completion will provide us partial outputs in `.csv` format.

    The folder `/src/out/finalq` contains some files. The csv file which is contained in the folder has the final output of the query.
10. If you wish to clean all the temporary files run `make clean`.
11. After cleaning the project if you wish to run again the run `make init` first to initialize the repo and then follow Steps 1 ... 9.

Known Issues:

Don't put spaces between commas and column names in select clause. Write `select column1,column2,column3...`