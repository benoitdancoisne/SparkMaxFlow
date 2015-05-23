run('random_graph_generation.m');
%% max flow

cm = sparse(ID_1, ID_2, caps, n, n);

[M,F,K] = graphmaxflow(cm,42,73,'Method', 'Edmonds');