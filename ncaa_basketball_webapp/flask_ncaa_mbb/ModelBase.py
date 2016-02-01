from sklearn.naive_bayes import GaussianNB


def base_model(XS, YS, totry):
    my_gnb = GaussianNB()
    my_gnb.fit(XS, YS)

    predict = [] 
    for mytry in totry:
        result =  my_gnb.predict_proba(mytry)
        predict.append(result[0][1])

    return predict


def final_prob(cdf1, cdf2):
    
    pdf1 = [0.0]
    pdf2 = [0.0]
    for ii in range(len(cdf1)-1):
        pdf1.append(abs(cdf1[ii+1]-cdf1[ii]))
        pdf2.append(abs(cdf2[ii+1]-cdf2[ii]))
    print sum(pdf1)
    print sum(pdf2)


    probs = [0.0]
    for ii in range(len(cdf1)-1):
        newprob = pdf1[ii]*pdf2[ii]
        probs.append(newprob)
    probssum = sum(probs)
    probs = [prob/probssum for prob in probs]

    return probs
