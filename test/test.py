'''
workload, threshold
wight = 0, i = 0
join_pair_list

phase1 解析工作负载，确定当前窗口内容
while(wight < threshold):
    q = workload[i]
    join_pairs = parse(q) #<t1, t2, a1, a2, f>
    join_pair_weight = (t1 * size(t1) + t2 * size(t2)) * f
    weight = wight + join_pair_weigth
    i = i + 1
    join_pair_list <- join_pairs

phase2 构建图，推荐数据分区建议
Graph <- join_pair_list
attribute_weight <- Graph
attribute_probility <- normalization(attribute_weight)
data_partitioning_recommendation <- PAA(attribute_probility)
'''

'''
BFS的方式
G, H #图G为已知分区建议的图，图H为需要给出分区建议的图，我们需要判断图H是否和图G相同
vertex_label #以匹配的顶点该值为1，否则为0
if number(V(G)) != number(V(H)) or number(E(G)) != number(E(H)): #如果两个图中边数目，顶点数目不同，则匹配失败
    return false
else:
    sort(E(H)), sort(E(G)) #将两图中的边按照权重进行排序
    e = max(E(H)), e' = max(E(G)) #e为图H中权重最大的边，e'为图G中权重最大的边
    # e = (u,v,w), e' = (u', v', w')
    if degree(u) == degree(u') and degree(v) == degree(v'): #如果两图中，边的对应顶点的度数相同
        picked_vertex_H <- u, picked_vertex_H <- v #将顶点存入已匹配顶点队列中
        picked_vertex_G <- u', picked_vertex_G <- v'
        vertex_label(u,v,u',v') = 1 #将顶点是否匹配的标记置为1
    else 
        return false #否则，匹配失败
    while picked_vertex not Empty: #只要已匹配顶点的队列不为空
        t = get_first(picked_vertex_H)  #从已匹配顶点队列中选出队头顶点
        t' = get_first(picked_vertex_G)
        E(t) = H edges adjacent to vertex t #找出与对头顶点相连的边
        E(t') = G edges adjacent to vertex t'
        sort(E(t)),sort(E(t')) #将这些边按照权重排序
        while E(t) not Empty and E(t') not Empty: 
            et = max(E(t)) # et = (ut,vt,wt) #选出权重最大的边
            et' = max(E(t')) # et' = (ut',vt',wt')
            if ((degree(ut) == degree(ut') and degree(vt) == degree(vt')) #如果最大的边在两个图中对应顶点度数相同
            or (degree(ut) == degree(vt') and degree(vt) == degree(ut'))):
                if (vertex_label(ut) == 0 and vertex_label(vt) == 0): #未存入已匹配顶点队列
                    picked_vertex_H <- ut, picked_vertex_H <- vt #存入队列
                    picked_vertex_G <- ut', picked_vertex_G <- vt'
                    vertex_label(ut,vt,ut',vt') = 1 #更改标志
            else:
                return false 否则，匹配失败
    return true 匹配成功
'''

'''

'''