def qparse(query:str) -> list:
    ''' Parse the query and send parts A and B \n
        A -> select , from , which and join clause --> parsed_out[0] \n
        B -> group by, having clause      --> parsed_out[1] \n
        C -> provides the table name --> parsed_out[2]'''
    parsed_out = []
    SPACE = ' '
    table = []

    #preprocess the query
    query = query.strip()
    s_q = query.split()
    kw = ['select', 'from', 'where', 'join', 'left', 'right', 'full', 'inner', 'group', 'order']
    for i in range(0, len(s_q)):
        if s_q[i].lower() in kw:
            s_q[i] = s_q[i].lower()

    # print(s_q)
    try:
        i_select = s_q.index('select')
        columns = s_q[i_select + 1]
        columns = remove_alias(columns)                
    except ValueError:
        raise ValueError

    try:
        i_from = s_q.index('from')
        table.append(s_q[i_from + 1])
    except ValueError:
        raise ValueError
    
    #extract end clause
    end_clause = ''
    i_end = -1
    try:
        i_group = s_q.index('group')
        i_end = i_group
        for i in range(i_group, len(s_q)):
            end_clause += SPACE+s_q[i]
    except ValueError:
        end_clause = ''
    
    if end_clause == '':
        try:
            i_order = s_q.index('order')
            i_end = i_order
            for i in range(i_order, len(s_q)):
                end_clause += SPACE+s_q[i]
        except ValueError:
            end_clause = ''
    
    #sanitize end clause
    end_clause = remove_alias(end_clause)
    
    # extract where clause
    where_cond = ''
    try:
        i_where = s_q.index('where')
        if end_clause == '':
            for i in range(i_where, len(s_q)):
                where_cond += SPACE+s_q[i]
        else:
            for i in range(i_where, i_end):
                where_cond += SPACE+s_q[i]
    except ValueError:
        i_where = -1
        where_cond = ''


    #extract join clause
    join_clause = ''
    if 'inner' in s_q:
        i_join = s_q.index('inner')
        table.append(s_q[i_join - 1])
        table.append(s_q[i_join + 2])
        table.append(s_q[i_join + 3])
    elif 'full' in s_q:
        i_join = s_q.index('full')
        table.append(s_q[i_join - 1])
        table.append(s_q[i_join + 2])
        table.append(s_q[i_join + 3])
    elif 'left' in s_q:
        i_join = s_q.index('left')
        table.append(s_q[i_join - 1])
        table.append(s_q[i_join + 2])
        table.append(s_q[i_join + 3])
    elif 'right' in s_q:  
        i_join = s_q.index('right')
        table.append(s_q[i_join - 1])
        table.append(s_q[i_join + 2])
        table.append(s_q[i_join + 3])
    elif 'join' in s_q:
        i_join = s_q.index('join')
        table.append(s_q[i_join - 1])
        table.append(s_q[i_join + 1])
        table.append(s_q[i_join + 2])
    else:
        i_join = -1

    if i_join == -1:
        join_clause = ''
    elif i_where == -1 and i_end != -1:
        for i in range(i_from+2, i_end):
            join_clause += SPACE+s_q[i]
    elif i_where != -1:
        for i in range(i_from+2, i_where):
            join_clause += SPACE+s_q[i]
    elif i_end == -1 and i_where == -1:
        for i in range(i_from+2, len(s_q)):
            join_clause += SPACE+s_q[i]

    #building the parts
    part_a = 'select'+SPACE+'*'+SPACE+'from'+SPACE+table[0]
    if join_clause != '':
        part_a += SPACE+join_clause
    if where_cond != '':
        part_a += SPACE+where_cond
    
    part_b = 'select'+SPACE+columns+SPACE+'from'+SPACE+table[0]
    if end_clause != '':
        part_b += end_clause

    parsed_out = [part_a, part_b, table]

    return parsed_out

# if __name__ == "__main__":
#     print(parse('SELECT * FROM table_name'))
#     print(parse('SELECT * FROM Customers WHERE CustomerID=1'))
#     print(parse('SELECT * FROM Customers WHERE City="Berlin" OR City="München"'))
#     print(parse('SELECT * FROM Customers WHERE NOT Country="Germany"'))
#     print(parse('SELECT Customers.CustomerName,Orders.OrderID FROM Customers LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID ORDER BY Customers.CustomerName'))
#     print(parse('SELECT COUNT(CustomerID), Country FROM Customers GROUP BY Country ORDER BY COUNT(CustomerID) DESC'))
#     print(parse('SELECT COUNT(CustomerID), Country FROM Customers GROUP BY Country HAVING COUNT(CustomerID) > 5 ORDER BY COUNT(CustomerID) DESC'))
def remove_alias(inp: str):

    tmp = inp
    
    try:
        i = 0
        while(True):
            i_dot = tmp.index('.')
            # print(i_dot)
            if i_dot < 2:
                tmp_tmp = tmp[i_dot+1:]
            else:
                tmp_tmp = tmp[:i_dot-1] + tmp[i_dot+1:]
            # print(tmp_tmp)
            tmp = tmp_tmp
            i += 1
    except ValueError:
        #remove * if present
        # print(tmp)
        try:
            tmp = tmp.replace('*,',',')
            tmp = tmp.replace(',*',',')
                
            # tmp = tmp.replace('*','')
            # print(tmp)
            #replace ',,' with ','
            # print(tmp)
            tmp = tmp.replace(',,',',')
            #remove if ','(comma) is 1st character
            if tmp[0] == ',':
                tmp = tmp[1:]
                # print(tmp)
            if tmp[len(tmp)- 1] == ',':
                tmp = tmp[:len(tmp) - 1]
        except IndexError:
            pass

    return tmp

if __name__ == "__main__":
    # print(remove_alias('order by b.ProductName'))
    # print(remove_alias('OrderID,sum(UnitPrice*Quantity*(1-Discount))'))
    # print(remove_alias('b.*,a.CategoryName'))
    # print(remove_alias('b.*,t.tribe,m.mribbee,c.*,d.fjfjf,b.o,c.*'))
    # print(qparse('select b.*,a.CategoryName from Categories a inner join Products b on a.CategoryID = b.CategoryID where b.Discontinued=1 order by b.ProductName'))
    print(qparse('select productID, productName, categoryID from products'))
    # removes aliasing of column names and end clause
    # aggregator cannot handle aliasing in column names and end clause--> Problem of sqlContext.sql(query) function
    # columns seprated by ',' should not have spaces
    # the columns names in select clause during join(aliasing) should not contain *. Must specify all the columns explicitly
    # '*' is removed from columns names if present during aliasing in join
    # doesnot allow sub-query execution
    # doesnot allow union, difference or other set operations