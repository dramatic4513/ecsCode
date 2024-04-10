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
workload, threshold
wight = 0, i = 0
CWW

phase1 解析工作负载，确定当前窗口内容
while(CWW < threshold):
    q = workload[i] #针对工作负载中第i个查询
    join_pairs = parse(q) #解析查询得到连接对信息<t1, t2, a1, a2, f> 
    join_pair_weight = (t1 * size(t1) + t2 * size(t2)) * f #计算连接对权重
    weight = wight + join_pair_weigth #计算当前窗口内权重之和
    i = i + 1 #指针指向下一个查询语句
    CWW <- join_pairs #将当前查询语句的连接信息添加到当前窗口内

phase2 构建图，推荐数据分区建议
Graph <- CWW #根据当前窗口信息构建图
attribute_weight <- Graph #根据图信息得到属性权重
attribute_probility <- normalization(attribute_weight) #对属性权重进行归一化得到属性概率
data_partitioning_recommendation ←PAA(attribute_probility)or HA(attribute_probility) #根据情况选择对应的算法给出数据分区建议
#基于属性权重的概率算法或者是混合算法

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
算法3.1 工作负载分析算法
输入：一个包含多条查询语句的特定工作负载 Q
输出：一个五元组集合 tacfSet //table attribute connection frequency
tacfSet <- {}
while Q not null do
    q <- Q[0]
    joinList <- parse(q)    //解析查询语句得到多表连接信息
    for join in joinList do
        t1,t2,a1,a2 <- join //得到参与连接的表和属性信息
        if t1,t2,a1,a2 in tacfSet then
            tacfSet[t1,t2,a1,a2].f += 1
        else 
            tacfSet <- tacfSet U {(t1, t2, a1, a2, 1)}
    endfor
    Q.remove(q)
endWhile
return tacfSet
'''

'''
算法 3.2 基于贪心的最大边权重匹配算法
输入： 带有边权重的无向多重图 Gwe
输出： 一个分区键组合 partitionKeyCombination
//phase1: 最大边匹配
while Gwe not null do
    e <- findMaximumEdge(Gwe) // e = (A,B,a,b,we)
    partitionKeyCombination.append(A.a, B.b)
    for e' in Gwe 
        if A ∈ e' or B ∈ e' then
            Gwe.remove(e)
    endfor
endWhile
//phase2: 剩余表分配
for table in Gwe
    if table not in partitionKeyCombination then
        attribute <- randomChoice(table.primaryKey)
        partitionKeyCombination.append(table.attribute)
endfor
return partitionKeyCombination
'''

'''
算法3.3 基于贪心的最大属性权重匹配算法
输入：带有属性权重的无向多重带权图 Gwa
输出：一个分区键组合 partitionKeyCombination
//phase1: 最大属性匹配
while Gwa not null do
    a <- findMaximumAttribute(Gwa)
    V <- findTableWithAttribute(a)
    for u in V
        partitionKeyCombination.append(u.a)
    endFor
    E <- findEdgeWithVertex
    Gwa.remove(V,E)
endWhile
//phase2: 剩余表分配
for table in Gwe
    if table not in partitionKeyCombination then
        attribute <- randomChoice(table.primaryKey)
        partitionKeyCombination.append(table.attribute)
endfor
return partitionKeyCombination        
'''

'''
算法3.4 混合分区算法
输入：一个带有边权重的无向多重带权图
输出：一种数据分区建议 partitionRecommendation
//phase1: 选出全表复制的表
tables <- Gwe //找出图中所有的表
redundancyUpperLimit //数据库整体冗余上限，由用户设置
preTables <- {} //满足收益大于冗余至少一个数量级的表
partitionRecommendation <- {}
redundancySum <- 0
for table in tables:
    if revenue(table)/redundancy(table) > 1000 then:
        preTables.append(table)
    endif
while preTables not null do
    for table in preTables
        redundancySum <- redundancySum + redundancy(table)
        if redundancySum < redundancyUpperLimit then
            partitionRecommendation.append(table.replication)
        else 
            redundancySum = redundancySum - redundancy(table)
        endif
    endfor
endwhile
for table in partitionRecommendation:
    Gwe.remove(table)
endfor
//phase2: 为剩余表分配分区键
    GEA(Gwe) or PEA(Gwe) or GAA(Gwe) or PAA(Gwe)
'''




'''
算法4.1 动态工作负载数据分区推荐算法
输入： 工作负载 workload 
输出： 分区建议 partitionRecommendation
weight = 0
while weight < threadshold do://解析工作负载，确定当前窗口内容
    q = workload[i]          //针对工作负载中的第i个查询
    join_pairs <- parse(q) //解析查询语句，得到连接对
    for join in join_pairs:
        weight = weight + join.weight
    endfor
    CWW.append[q]
endWhile
partitionRecommendation <- PAA(CWW)
'''

'''
算法4.2 图匹配算法（VEWM）
输入：图G,图H 判断图H是否和图G有着相同的拓扑结构和边偏序关系
vertexLable #已匹配的顶点，该值为1，否则为0
if number(V(G)) != number(V(H)) or number(E(G)) != number(E(H)) then
    return false
else 
    sort(E(H)), sort(E(G))
    eh = max(E(H)), eg = max(E(G))
    if degree(hv,hu) == degree(gv, gu) then
        pickedVertxH <- (hu, hv)
        pickedVertxG <- (gu, gv)
        vertexLable.set(hu,hv,gu,gv) = 1
    else 
        return false
    while pickedVertxH not null and pickedVertxG not null 
        
'''
'''
BFS的方式 （如果有多条权重相同的边时，用状态机回溯匹配。类似VF2？）
G, H #图G为已知分区建议的图，图H为需要给出分区建议的图，我们需要判断图H是否和图G相同
vertex_label #已匹配的顶点该值为1，否则为0
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









