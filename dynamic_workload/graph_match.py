from queue import Queue
# 图匹配

five = [
    ['item', 'i_item_sk', 'store_sales', 'ss_item_sk', 433569792],
    ['date_dim', 'd_date_sk', 'store_sales', 'ss_sold_date_sk', 435019776],
    ['customer_address', 'ca_address_sk', 'store_sales', 'ss_addr_sk', 432414720],
    ['catalog_sales', 'cs_item_sk', 'item', 'i_item_sk', 316489728],
    ['catalog_sales', 'cs_sold_date_sk', 'date_dim', 'd_date_sk', 317939712],
    ['customer_address', 'ca_address_sk', 'catalog_sales', 'cs_bill_addr_sk', 315334656],
    ['item', 'i_item_sk', 'web_sales', 'ws_item_sk', 163807232],
    ['date_dim', 'd_date_sk', 'web_sales', 'ws_sold_date_sk', 165257216],
    ['customer_address', 'ca_address_sk', 'web_sales', 'ws_bill_addr_sk', 162652160]]


class Graph:
    table_attribute_index_dict = dict() # 用于存储字符串“表名+属性名” 与 索引的对应关系
    index_table_attribute_dict = dict() # 索引 和 “表名 + 属性名” 的对应关系
    matrix_value = []


'''
输入： 五元组
输出： 一个图对象
'''
def build_graph(five):
    five_set = set()
    for i in five:
        ta1 = i[0] + "+" + i[1]
        ta2 = i[2] + "+" + i[3]
        five_set.add(ta1)
        five_set.add(ta2)

    ta_ind = dict() #表名+属性名 ： 索引
    ind_ta = dict() #索引 ： 表名+属性名

    index = 0
    for i in five_set:
        ta_ind[i] = index
        ind_ta[index] = i
        index = index + 1

    matrix = [[0 for _ in range(len(five_set))] for _ in range(len(five_set))]  # 用于存储权重值
    for i in five:
        ta1 = i[0] + "+" + i[1]
        ta2 = i[2] + "+" + i[3]
        ind1 = ta_ind[ta1]
        ind2 = ta_ind[ta2]
        matrix[ind1][ind2] = i[4]
        matrix[ind2][ind1] = i[4]
    graph = Graph()
    graph.table_attribute_index_dict = ta_ind
    graph.index_table_attribute_dict = ind_ta
    graph.matrix_value = matrix
    return graph

'''
比较两个图是否一致
'''
def graph_compare(graph1: Graph, graph2: Graph):

    table_dict = dict()  #用于存储两个表的顶点对应情况
    table_attribute_dict = dict() #用于存储两个表+属性的顶点对应情况

    table1_queue = Queue() #存储第一个图中处于顶点队列中的顶点
    table2_queue = Queue()

    #找到权重最大的边的位置
    max_value = -1
    max_index_1 = (0,0)
    for i, row in enumerate(graph1.matrix_value):
        for j, element in enumerate(row):
            if element > max_value:
                max_value = element
                max_index_1 = (i, j)

    max_value = -1
    max_index_2 = (0, 0)
    for i, row in enumerate(graph2.matrix_value):
        for j, element in enumerate(row):
            if element > max_value:
                max_value = element
                max_index_2 = (i, j)


    table1 = graph1.index_table_attribute_dict[max_index_1[0]].split('+')[0]
    table2 = graph1.index_table_attribute_dict[max_index_1[1]].split('+')[0]
    table3 = graph2.index_table_attribute_dict[max_index_2[0]].split('+')[0]
    table4 = graph2.index_table_attribute_dict[max_index_2[1]].split('+')[0]
    print(table1, table2, table3, table4)
    graph1.matrix_value[max_index_1[0]][max_index_1[1]] = -1
    graph2.matrix_value[max_index_2[0]][max_index_2[1]] = -1 #将已匹配成功的边的权重值置为-1

    # 根据顶点的边的条数确定对应顶点
    table1_edge_number = 0
    table2_edge_number = 0
    table3_edge_number = 0
    table4_edge_number = 0

    for i in range(len(graph1.matrix_value)):
        table = graph1.index_table_attribute_dict[i].split('+')[0]
        if table == table1:
            table1_edge_number += 1
        elif table == table2:
            table2_edge_number += 1

    for i in range(len(graph2.matrix_value)):
        table = graph2.index_table_attribute_dict[i].split('+')[0]
        if table == table3:
            table3_edge_number += 1
        elif table == table4:
            table4_edge_number += 1

    if table1_edge_number == table3_edge_number and table2_edge_number == table4_edge_number :
        table_dict[table1] = table3
        table_dict[table2] = table4
        table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[0]]] = graph2.index_table_attribute_dict[max_index_2[0]]
        table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[1]]] = graph2.index_table_attribute_dict[max_index_2[1]]
        table1_queue.put(table1)
        table1_queue.put(table2)
        table2_queue.put(table3)
        table2_queue.put(table4)
    elif table1_edge_number == table4_edge_number and table2_edge_number == table3_edge_number:
        table_dict[table1] = table4
        table_dict[table2] = table3
        table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[0]]] = graph2.index_table_attribute_dict[max_index_2[1]]
        table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[1]]] = graph2.index_table_attribute_dict[max_index_2[0]]
        table1_queue.put(table1)
        table1_queue.put(table2)
        table2_queue.put(table4)
        table2_queue.put(table3)
    else:
        print(" max weight edge vertex is not match") #最大顶点的边不匹配
        return False

    while(table1_queue.empty() == False and table2_queue.empty() == False):
        #选出对应顶点中权重最大的边
        cur_table1 = table1_queue.get()
        cur_table2 = table2_queue.get()
        max_index_1 = (0, 0)
        max_weight1 = -1
        max_index_2 = (0, 0)
        max_weight2 = -1
        for i in range(len(graph1.matrix_value)):
            table = graph1.index_table_attribute_dict[i].split('+')[0]
            if table == cur_table1:
                for j, element in enumerate(graph1.matrix_value[i]):
                    if element > max_weight1:
                        max_weight1 = element
                        max_index_1 = (i, j)
        if max_weight1 == -1:
            continue

        for i in range(len(graph2.matrix_value)):
            table = graph2.index_table_attribute_dict[i].split('+')[0]
            if table == cur_table2:
                for j, element in enumerate(graph2.matrix_value[i]):
                    if element > max_weight2:
                        max_weight2 = element
                        max_index_2 = (i, j)
        if max_weight2 == -1:
            print("-1 162")
            return False

        table1 = graph1.index_table_attribute_dict[max_index_1[0]].split('+')[0]
        table2 = graph1.index_table_attribute_dict[max_index_1[1]].split('+')[0]
        table3 = graph2.index_table_attribute_dict[max_index_2[0]].split('+')[0]
        table4 = graph2.index_table_attribute_dict[max_index_2[1]].split('+')[0]
        print(table1, table2, table3, table4)
        graph1.matrix_value[max_index_1[0]][max_index_1[1]] = -1
        graph2.matrix_value[max_index_2[0]][max_index_2[1]] = -1  # 将已匹配成功的边的权重值置为-1

        # 根据顶点的边的条数确定对应顶点
        table1_edge_number = 0
        table2_edge_number = 0
        table3_edge_number = 0
        table4_edge_number = 0

        for i in range(len(graph1.matrix_value)):
            table = graph1.index_table_attribute_dict[i].split('+')[0]
            if table == table1:
                table1_edge_number += 1
            elif table == table2:
                table2_edge_number += 1

        for i in range(len(graph2.matrix_value)):
            table = graph2.index_table_attribute_dict[i].split('+')[0]
            if table == table3:
                table3_edge_number += 1
            elif table == table4:
                table4_edge_number += 1

        if table1_edge_number == table3_edge_number and table2_edge_number == table4_edge_number:
            table_dict[table1] = table3
            table_dict[table2] = table4
            if graph1.index_table_attribute_dict[max_index_1[0]] in table_attribute_dict and  table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[0]]] != graph2.index_table_attribute_dict[
                max_index_2[0]]:
                return False
            table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[0]]] = graph2.index_table_attribute_dict[
                max_index_2[0]]
            if graph1.index_table_attribute_dict[max_index_1[1]] in table_attribute_dict and table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[1]]] != graph2.index_table_attribute_dict[
                max_index_2[1]]:
                return False
            table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[1]]] = graph2.index_table_attribute_dict[
                max_index_2[1]]
            table1_queue.put(table1)
            table1_queue.put(table2)
            table2_queue.put(table3)
            table2_queue.put(table4)
        elif table1_edge_number == table4_edge_number and table2_edge_number == table3_edge_number:
            table_dict[table1] = table4
            table_dict[table2] = table3
            if graph1.index_table_attribute_dict[max_index_1[0]] in table_attribute_dict and table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[0]]] != graph2.index_table_attribute_dict[
                max_index_2[1]]:
                return False
            table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[0]]] = graph2.index_table_attribute_dict[
                max_index_2[1]]
            if graph1.index_table_attribute_dict[max_index_1[1]] in table_attribute_dict and table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[1]]] != graph2.index_table_attribute_dict[
                max_index_2[0]]:
                return False
            table_attribute_dict[graph1.index_table_attribute_dict[max_index_1[1]]] = graph2.index_table_attribute_dict[
                max_index_2[0]]
            table1_queue.put(table1)
            table1_queue.put(table2)
            table2_queue.put(table4)
            table2_queue.put(table3)
        else:
            print(" max weight edge vertex is not match")  # 最大顶点的边不匹配
            return False

    return True




if __name__ == '__main__':
    graph1 = build_graph(five)
    graph2 = build_graph(five)
    ans = graph_compare(graph1, graph2)
    print(ans)






