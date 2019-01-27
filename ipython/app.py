# Librairies
import numpy as np
from scipy import sparse
import os
from pyspark import SparkContext, SparkConf
import matplotlib.pyplot as plt


# Function pour la creation d'un RDD du type (userID, movieID, rating)
def parseRating(line):
    fields = line.split('::')
    return int(fields[0]), int(fields[1]), float(fields[2])

#Calcul du rating predit
def predictedRating(x, P, Q):
    return( Q[x[1]-1,:].dot(P[x[0]-1,:]))

# Calcul de l'erreur MSE
def computeMSE(rdd, P, Q):
    mse = rdd.map(lambda r: (r[2]-predictedRating(r, P, Q))**2).mean()
    return(mse)

def GD(trainRDD, K=10, MAXITER=50, GAMMA=0.001, LAMBDA=0.05):
    # Construction de la matrice R (creuse)
    row=[]
    col=[]
    data=[]
    for part in trainRDD.collect():
        row.append(part[0]-1)
        col.append(part[1]-1)
        data.append(part[2])
    R=sparse.csr_matrix((data, (row, col)))

    # Initialisation aleatoire des matrices P et Q
    M,N = R.shape
    P = np.random.rand(M,K)
    Q = np.random.rand(N,K)

    # Calcul de l'erreur MSE initiale
    mse=[]
    mse_tmp = computeMSE(trainRDD, P, Q)
    mse.append([0, mse_tmp])
    print("epoch: ", str(0), " - MSE: ", str(mse_tmp))

    # Boucle
    nonzero = R.nonzero()
    nbNonZero = R.nonzero()[0].size
    I,J = nonzero[0], nonzero[1]
    for epoch in range(MAXITER):
        print('epoch:', epoch)
        for i,j in zip(I,J):
            # Mise a jour de P[i,:] et Q[j,:] par descente de gradient a pas fixe
            coef = -(R[i,j] - Q[j,:].dot(P[i,:]))
            dP = 2*( coef*Q[j,:] + LAMBDA*P[i,:])
            dQ = 2*( coef*P[i,:] + LAMBDA*Q[j,:])
            P[i,:] = P[i,:] - GAMMA * dP
            Q[j,:] = Q[j,:] - GAMMA * dQ
        # Calcul de l'erreur MSE courante, et sauvegarde dans le tableau mse
        mse.append([epoch,computeMSE(trainRDD, P, Q)])
    return(P, Q, mse)


#main
if __name__ == '__main__':
    # Important variables
    movieLensHomeDir="data/"
    conf = SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Matrix Factorization")
    sc = SparkContext(conf = conf)

    #initial RDD
    ratingsRDD = sc.textFile(movieLensHomeDir + "ratings.dat").map(parseRating).setName("ratings").cache()
    # Calcul du nombre de ratings
    numRatings = ratingsRDD.count()
    # Calcul du nombre d'utilisateurs distincts
    numUsers = ratingsRDD.map(lambda r: r[0]).distinct().count()
    # Calcul du nombre de films distincts
    numMovies = ratingsRDD.map(lambda r: r[1]).distinct().count()
    print("We have %d ratings from %d users on %d movies.\n" % (numRatings, numUsers, numMovies))

    #Dimensions de la matrice R
    M = ratingsRDD.map(lambda r: r[0]).max()
    N = ratingsRDD.map(lambda r: r[1]).max()
    matrixSparsity = float(numRatings)/float(M*N)
    print("We have %d users, %d movies and the rating matrix has %f percent of non-zero value.\n" % (M, N, 100*matrixSparsity))

    # Separation du jeu de donnees en un jeu d'apprentissage et un jeu de test
    # Taille du jeu d'apprentissage (en %)
    learningWeight = 0.7

    # Creation des RDD "apprentissage" et "test" depuis la fonction randomsplit
    trainRDD, testRDD = ratingsRDD.randomSplit([learningWeight,1-learningWeight])
    numTrain = trainRDD.count()
    numTest = testRDD.count()
    print("The training set ratio : ",numTrain*1.0 /(numTrain + numTest))
    print("\n")

    #Optimization : decomposition de rang minimal
    P,Q,mse = GD(trainRDD, K=10, MAXITER=3, GAMMA=0.001, LAMBDA=0.05)

    ##Calcul et affichage des ratings predits
    predRDD = testRDD.map(lambda r : (r[0],r[1],r[2],predictedRating(r, P, Q)))
    print(predRDD.sample(False, 0.1,).take(5))
    print("\n")

    # Affichage de l'erreur MSE
    epoch = [ x[0] for x in mse]
    mse_value = [ x[1] for x in mse]
    plt.plot(epoch,mse_value)
    plt.xlabel('Epoch')
    plt.ylabel('MSE')
    plt.title("Erreur MSE en fonction de l'epoque")
    plt.show()

    #Stop Spark
    spark.stop()
