M = LOAD '$M' USING PigStorage(',') AS (i, j, v);
N = LOAD '$N' USING PigStorage(',') AS (k, l, w);
J = JOIN M BY j, N BY k;
R1 = FOREACH J GENERATE i, l, (v*w) AS V;
R2 = GROUP R1 BY (i, l);
RES = FOREACH R2 GENERATE group.$0, group.$1, SUM(R1.V);
STORE RES INTO '$O' USING PigStorage (',');
