   

int_models = ["Hist_Relations","Daily_MV","AUM_Pivot"]
    
size = len(int_models)

pairings = [] #define pairings to establish a relation

for  idx,model in enumerate(int_models):
    if idx + 1 <  size:
        i = int_models[idx] 
        j = int_models[idx+1]

        pairings.append( tuple((i,j)) )

print(pairings)