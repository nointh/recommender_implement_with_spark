
import numpy as np
from numpy.linalg import inv
import matplotlib.pyplot as plt

class DSGD:
    def __init__(self, num_factor, step_size, max_iter, lambd) -> None:
        self.step_size = step_size
        self.max_iter = max_iter
        self.lambd = lambd
        self.num_factor = num_factor
        self.train_rmse_arr = []
        self.test_rmse_arr = []

    def SGD(self, keyed_iterable, stepSize, numFactors,lam, mse, nUpdates):
        iterlist = (next(keyed_iterable))
        key = iterlist[0]
        Miter = iterlist[1][0]
        Hiter = iterlist[1][1]
        Witer = iterlist[1][2]
        
        Wdict = {}
        Hdict = {}
        for h in Hiter:
            Hdict[h[0]] = h[1]
        
        for w in Witer:
            Wdict[w[0]] = w[1]
        counter = 1
        for m in Miter:
            (i,j,rat) = m
            if i not in Wdict:
                Wdict[i] = tuple([i,np.random.uniform(0,1,numFactors).astype('float32')])
            if j not in Hdict:
                Hdict[j] = tuple([j,np.random.uniform(0,1,numFactors).astype('float32')])

            (Nw, Wprev) = Wdict[i]
            (Nh, Hprev) = Hdict[j]
            error = (rat - np.dot(Wprev, Hprev.T))
            mse += pow((error[0][0]), 2)
            
            Wnew = Wprev - stepSize.value*(-2*error*Hprev+ (2.0*lam.value)*Wprev)
            Hnew = Hprev - stepSize.value*(-2*error*Wprev + (2.0*lam.value)*Hprev)
            nUpdates += 1
            Wdict[i] = tuple([Nw, Wnew])
            Hdict[j] = tuple([Nh, Hnew])
            
        return (tuple(['W',Wdict.items()]), tuple(['H',Hdict.items()]))

    def get_rmse(self, R, w, h):
        sse = R.map(lambda x: (x[2] - w.value[x[0]].dot(h.value[x[1]].T))**2 ).reduce(lambda x,y: x+y)
        count = R.count()
        rmse = pow(sse/count, 0.5)
        return rmse

    def assignBlockIndex (self, index, numData, numWorkers):
        blockSize = numData/numWorkers
        if(numData % numWorkers != 0): blockSize = blockSize + 1
        return int(np.floor(index/np.ceil(blockSize)))+1

    def train(self, sc, originRDD, trainRDD, testRDD):
        self.train_rmse_arr = []
        self.test_rmse_arr = []
        numFactors = self.num_factor
        numWorkers = sc.defaultParallelism
        stepSize = self.step_size
        max_iter = self.max_iter
        numRows = originRDD.map(lambda x: x[0]).distinct().count()
        numCols = originRDD.map(lambda x: x[1]).distinct().count()
        W = originRDD.map(lambda x: tuple([int(x[0]),1])).reduceByKey(lambda x,y : x+y).map(lambda x: tuple([x[0], tuple([x[1], np.random.rand(1,numFactors).astype('float16')])])).persist()
        H = originRDD.map(lambda x: tuple([int(x[1]),1])).reduceByKey(lambda x,y : x+y).map(lambda x: tuple([x[0], tuple([x[1], np.random.rand(1,numFactors).astype('float16')])])).persist()
        Vblocked = trainRDD.keyBy(lambda x: self.assignBlockIndex(x[0], numRows, numWorkers)).partitionBy(numWorkers)
        

        #init first time rmse
        Wvec = W.map(lambda x: (x[0], x[1][1])).collect()
        w_broadcast = sc.broadcast(dict(Wvec))
        Hvec = H.map(lambda x: (x[0], x[1][1])).collect()
        h_broadcast = sc.broadcast(dict(Hvec))

        train_rmse = self.get_rmse(trainRDD, w_broadcast, h_broadcast)[0,0]
        test_rmse = self.get_rmse(testRDD, w_broadcast, h_broadcast)[0,0]
        self.train_rmse_arr.append(train_rmse)
        self.test_rmse_arr.append(test_rmse)

        stepSize = sc.broadcast(stepSize)
        for it in range(max_iter):
            mse = sc.accumulator(0.0)
            nUpdates = sc.accumulator(0)
            lam = sc.broadcast(0.01)

            stepSize = sc.broadcast(stepSize.value * 0.9)
            #generate random strata

            perms = np.random.permutation(numWorkers)+1
            perms_dict = {i: val for i, val in enumerate(perms)}
            rev_perms=list(i+1 for i in (dict(sorted(perms_dict.items(), key=lambda item: item[1]))).keys())
            
            Vfilt = Vblocked.filter(lambda x: perms[x[0]-1]==self.assignBlockIndex(x[1][1],numCols,numWorkers)).persist()
            
            Hblocked = H.keyBy(lambda x: rev_perms[self.assignBlockIndex(x[0], numCols, numWorkers)-1])
            Wblocked = W.keyBy(lambda x: self.assignBlockIndex(x[0], numRows, numWorkers))
            
            groupRDD = Vfilt.groupWith(Hblocked, Wblocked).partitionBy(numWorkers)
            
            WH = groupRDD.mapPartitions(lambda x: self.SGD(x, stepSize, numFactors,lam, mse, nUpdates))
            W.unpersist()
            H.unpersist()
            W = WH.filter(lambda x: x[0]=='W').flatMap(lambda x: x[1]).persist()
            H = WH.filter(lambda x: x[0]=='H').flatMap(lambda x: x[1]).persist()
            Wvec = W.map(lambda x: (x[0], x[1][1])).collect()
            w_broadcast = sc.broadcast(dict(Wvec))
            Hvec = H.map(lambda x: (x[0], x[1][1])).collect()
            h_broadcast = sc.broadcast(dict(Hvec))
            rmse = np.sqrt(mse.value/nUpdates.value)
            train_rmse = self.get_rmse(trainRDD, w_broadcast, h_broadcast)[0,0]
            test_rmse = self.get_rmse(testRDD, w_broadcast, h_broadcast)[0,0]

            self.train_rmse_arr.append(train_rmse)
            self.test_rmse_arr.append(test_rmse)
            print("MSE/update for {}-th iteration is: {}/{} ".format(it, mse.value, nUpdates.value))
            print("RMSE: {}".format(rmse))
            print("Global RMSE: {}".format(train_rmse))
    def get_train_rmse(self):
        return self.train_rmse_arr

    def get_test_rmse(self):
        return self.test_rmse_arr
    def plot_rmse(self):
        plt.plot(self.get_train_rmse(), label="train loss")
        plt.plot(self.get_test_rmse(), label="test loss")
        plt.legend()
        plt.show()

class ALS:
    def __init__(self, num_factor, max_iter, lambd) -> None:
        self.max_iter = max_iter
        self.lambd = lambd
        self.num_factor = num_factor

    def getRelativeIndex(self, value, index_list):
        return index_list[value]
    
    def computeOptimizeMatrix(self, iterables, constant_matrix_broadcast, lamb):
        fixed_matrix = constant_matrix_broadcast.value
        num_factors = fixed_matrix.shape[0]
        iter_dict = dict(iterables)
        X = fixed_matrix[:, list(iter_dict.keys())]
        R = np.matrix(list(iter_dict.values()))
        XtX = X.dot(X.T)
        RX = X.dot(R.T)
        return np.linalg.solve(XtX + lamb.value * np.eye(num_factors),RX)

    def get_rmse(self, R, w, h, sorted_users, sorted_items):
        sse = R.map(lambda x: self.get_error_square(x[2], w,h, sorted_users[x[0]], sorted_items[x[2]]) ).reduce(lambda x,y: x+y)
        count = R.count()
        rmse = pow(sse/count, 0.5)
        return rmse
    def get_error_square(self, rating, w, h, i, j):
        pred = w[:, [i]].T.dot(h[:, [j]])[0, 0]
        return (rating - pred)**2

    def train(self, sc, originRDD, trainRDD, testRDD):
        numFactors = self.num_factor
        sorted_users = dict(originRDD.map(lambda x: x[0]).distinct().sortBy(lambda idx: idx, ascending = True)\
            .zipWithIndex().collect())

        sorted_items = dict(originRDD.map(lambda x: x[1]).distinct().sortBy(lambda idx: idx, ascending = True)\
            .zipWithIndex().collect())

        item_count = len(sorted_items)
        user_count = len(sorted_users)
        M = trainRDD.map(lambda l: (l[0], (l[1], l[2])))\
            .map(lambda x: (self.getRelativeIndex(x[0], sorted_users), (self.getRelativeIndex(x[1][0], sorted_items), x[1][1])))
        
        W = np.matrix(np.random.rand(numFactors, user_count))
        H = np.matrix(np.random.rand(numFactors, item_count))

        R_u = M.map(lambda x: (x[0], (x[1][0], x[1][1]))).cache()
        R_i = M.map(lambda x: (x[1][0], (x[0], x[1][1]))).cache()

        w_broadcast = sc.broadcast(W)
        h_broadcast = sc.broadcast(H)
        lambda_broadcast = sc.broadcast(self.lambd)
        print(w_broadcast.value.shape)
        print(h_broadcast.value.shape)
        for i in range(self.max_iter):
            newW = dict(R_u.groupByKey()\
                .mapValues(lambda row:self.computeOptimizeMatrix(row,h_broadcast,lambda_broadcast))\
                .sortByKey()\
                .collect())
            for key, val in newW.items():
                W[:, key] = val[:,0]
            #W = np.array(list(map(lambda x: np.array(x.flatten())[0], newW))).T
            w_broadcast.destroy()
            w_broadcast = sc.broadcast(W)
            newH = dict(R_i.groupByKey()\
                .mapValues(lambda row: self.computeOptimizeMatrix(row,w_broadcast, lambda_broadcast))\
                .sortByKey()\
                .collect())
            for key, val in newH.items():
                H[:, key] = val[:,0]
            #H = np.array(list(map(lambda x: np.array(x.flatten())[0], newH))).T
            h_broadcast.destroy()
            h_broadcast = sc.broadcast(H)
            
            # sse = M.map(lambda x: get_error_square(x[1][1], x[0], x[1][0])).reduce(lambda x,y: x+y)[0,0]
            # count = M.count()
            # mse = pow((sse/count), 0.5)
            rmse = self.get_rmse(trainRDD, W, H, sorted_users, sorted_items)
            print("Iteration %d:" % i)
            print("\nRMSE: %5.4f\n" % rmse)
