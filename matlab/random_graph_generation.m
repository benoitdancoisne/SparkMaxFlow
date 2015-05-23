clear;

% set seed
rng(1);

n = 100;
m = 1000;

maxCap = 42;

ID_1 = zeros(1, m);
ID_2 = zeros(1, m);

for i=1:m
    t = 0;
    while (t == 0)
        k = randi([1, n]);
        l = randi([1, n]);
        if k < l
            ID_1(i) = k;
            ID_2(i) = l;
            t = 1;
        end
    end
end

caps = randi([1, maxCap], 1, m);

A = [ID_1; ID_2; caps];

%% write file
fileID = fopen('..\data\toy-edges-big.txt','w');
fprintf(fileID,'%.0f %.0f %.0f\n',A);
fclose(fileID);

IDs = 1:n;
fileID2 = fopen('..\data\toy-vertices-big.txt','w');
fprintf(fileID2,'%.0f ', IDs);
fclose(fileID2);