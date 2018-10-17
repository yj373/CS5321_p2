SELECT * FROM Sailors;
SELECT Sailors.A FROM Sailors;
SELECT Boats.F, Boats.D FROM Boats;
SELECT * FROM Sailors WHERE Sailors.B >= Sailors.C;
SELECT Reserves.G, Reserves.H FROM Reserves;
SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C
SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C AND Sailors.B < Sailors.C;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;