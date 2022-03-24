package shared

package object predictions
{
  case class Rating(user: Int, item: Int, rating: Double)


  // ===== Initial utility code =====
  
  def timingInMs(f: () => Double): (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end - start) / 1e6)
  }

  def mean(s: Seq[Double]): Double =  {
    if (s.size > 0) 
      s.reduce(_+_) / s.length 
    else 
      0.0
  }

  def std(s: Seq[Double]): Double = {
    if (s.size == 0) 
      0.0
    else {
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble)
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def load(spark: org.apache.spark.sql.SparkSession, path: String, sep: String): org.apache.spark.rdd.RDD[Rating] = {
       val file = spark.sparkContext.textFile(path)
       return file
         .map(l => {
           val cols = l.split(sep).map(_.trim)
           toInt(cols(0)) match {
             case Some(_) => Some(Rating(cols(0).toInt, cols(1).toInt, cols(2).toDouble))
             case None => None
           }
       })
         .filter({ case Some(_) => true 
                   case None => false })
         .map({ case Some(x) => x 
                case None => Rating(-1, -1, -1)})
  }



  // ===== Custom types =====

  type RatingArr = Array[shared.predictions.Rating]
  type DistrRatingArr = org.apache.spark.rdd.RDD[Rating]
  type SimilarityFunc = (Int, Int, RatingArr) => Double
  type RatingPredFunc = (Int, Int) => Double
  type TrainerOfPredictor = (RatingArr) => RatingPredFunc

   
  // ===== MAE calculation =====

  def getMAE(val1: Double, val2: Double): Double = {
    scala.math.abs(val1 - val2)
  }

  def calcDatasetMAE(dataset: RatingArr, predictor: RatingPredFunc): Double = {
    val err_glob_avg = dataset.map(rev => getMAE(rev.rating, predictor(rev.user, rev.item)))
    return mean(err_glob_avg)
  }

  // make a function that takes a trainer of predictor 
  // and outputs MAE on given datasets for this predictor
  def getFuncCalcMAE(train: RatingArr, test: RatingArr): TrainerOfPredictor => Double = {
    return (train_pred_func: TrainerOfPredictor) => calcDatasetMAE(test, train_pred_func(train))
  }
  
  // make a function that takes a trainer of predictor 
  // and outputs timings of MAE calculation on given datasets for this predictor
  def getFuncCalcMAETimings(train: RatingArr, test: RatingArr, num_runs: Int): TrainerOfPredictor => Seq[Double] = {
    def calcMAE = getFuncCalcMAE(train, test)

    return (train_pred_func: TrainerOfPredictor) => 
      // repeat the required number of times
      (1 to num_runs)
      // compute MAE values and execution times
      .map(x => timingInMs(() => calcMAE(train_pred_func)))
      // extract times only
      .map(_._2)
  }



  // ===== (old) Custom timing functions =====
 
  def my_timingInMs[Arr](f: (Arr, Arr) => Double, train: Arr, test: Arr): (Double, Double) = {
    val start = System.nanoTime() 
    val output = f(train, test)
    val end = System.nanoTime()
    return (output, (end - start) / 1e6)
  }

  def getMeasurements[Arr](func: (Arr, Arr) => Double, train: Arr, test: Arr, runs: Int): Seq[(Double, Double)] = {
    return (1 to runs).map(x => my_timingInMs(func, train, test))
  }

  def getTimings[Arr](func: (Arr, Arr) => Double, train: Arr, test: Arr, runs: Int): Seq[Double] = {
    return getMeasurements(func, train, test, runs).map(t => t._2)
  }



  // ===== Baseline predictions =====

  // Computations for single instances

  def globalAvgRating(dataset: RatingArr): Double = {
    mean(dataset.map(_.rating))
  }

  def userAvgRating(dataset: RatingArr, userId: Int): Double = {
    val user_reviews = dataset.filter(_.user == userId)

    if (user_reviews.isEmpty)
      globalAvgRating(dataset)
    else
      globalAvgRating(user_reviews)
  }

  def itemAvgRating(dataset: RatingArr, itemId: Int): Double = {
    val item_reviews = dataset.filter(_.item == itemId)

    if (item_reviews.isEmpty)
      globalAvgRating(dataset)
    else
      globalAvgRating(item_reviews)
  }

  def scaleRatingToUserAvg(rating: Double, avgRating: Double): Double = {
    if (rating > avgRating)
      5.0 - avgRating
    else if (rating < avgRating)
      avgRating - 1.0
    else
      1.0
  }

  def normalizedDev(review: Rating, avg_user_rating: Double): Double = {
    return (review.rating - avg_user_rating) / scaleRatingToUserAvg(review.rating, avg_user_rating)
  }

  def itemAvgDev(dataset: RatingArr, itemId: Int): Double = {
    val item_reviews = dataset.filter(_.item == itemId)
    val users_that_rated = item_reviews.map(_.user).distinct

    val avg_user_rating_map = users_that_rated.map(
      user => (user, userAvgRating(dataset, user))
    ).toMap
    
    val norm_devs = item_reviews.map(
      review => normalizedDev(review, avg_user_rating_map(review.user))
    )
    return mean(norm_devs)
  }

  def baselinePrediction(user_avg: Double, item_dev: Double): Double = {
    return user_avg + item_dev * scaleRatingToUserAvg(user_avg + item_dev, user_avg)
  }

  def baselineRating(dataset: RatingArr, userId: Int, itemId: Int): Double = {
    val user_avg = userAvgRating(dataset, userId)
    val mean_item_dev = itemAvgDev(dataset, itemId)
    return baselinePrediction(user_avg, mean_item_dev)
  }


  // Computations for datasets

  def userAvgMap(dataset: RatingArr): Map[Int, Double] = {
    val glob_avg = globalAvgRating(dataset)

    // similar to preprocDataset
    val user_avg_map = dataset
      .map(review => (review.user, review.rating))
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .mapValues(mean(_))
      .toMap
      .withDefaultValue(glob_avg)

    return user_avg_map
  }

  def itemAvgMap(dataset: RatingArr): Map[Int, Double] = {
    val glob_avg = globalAvgRating(dataset)

    // similar to preprocDataset
    val user_avg_map = dataset
      .map(review => (review.item, review.rating))
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .mapValues(mean(_))
      .toMap
      .withDefaultValue(glob_avg)

    return user_avg_map
  }

  def predictorGlobalAvg(dataset: RatingArr): RatingPredFunc = {
    val glob_avg = globalAvgRating(dataset)
    return (user: Int, item: Int) => glob_avg
  }

  def predictorUserAvg(dataset: RatingArr): RatingPredFunc = {
    val user_avg_map = userAvgMap(dataset)
    return (user: Int, item: Int) => user_avg_map(user)
  }

  def predictorItemAvg(dataset: RatingArr): RatingPredFunc = {
    val user_avg_map = userAvgMap(dataset)
    return (user: Int, item: Int) => user_avg_map(item)
  }

  def predictorBaseline(dataset: RatingArr): RatingPredFunc = {
    val user_avg_map = userAvgMap(dataset)

    val glob_avg = globalAvgRating(dataset)

    // can be optimized
    val all_items = dataset.map(_.item).distinct
    val item_dev_map = all_items
      .map(item => (item, itemAvgDev(dataset, item)))
      .toMap
      .withDefaultValue(glob_avg)

    return (user: Int, item: Int) => baselinePrediction(user_avg_map(user), item_dev_map(item))
  }



  // ===== Distributed prediction =====

  def distr_globalAvgRating(dataset: DistrRatingArr): Double = {
    dataset.map(x => x.rating).mean
  }

  def distr_userAvgRating(dataset: DistrRatingArr, userId: Int): Double = {
    val user_reviews = dataset.filter(x => x.user == userId)

    if (user_reviews.isEmpty())
      distr_globalAvgRating(dataset)
    else
      distr_globalAvgRating(user_reviews)
  }

  def distr_itemAvgRating(dataset: DistrRatingArr, itemId: Int): Double = {
    val item_reviews = dataset.filter(x => x.item == itemId)

    if (item_reviews.isEmpty())
      distr_globalAvgRating(dataset)
    else
      distr_globalAvgRating(item_reviews)
  }

  def distr_itemAvgDev(dataset: DistrRatingArr, itemId: Int): Double = {
    val glob_avg = distr_globalAvgRating(dataset)
    val just_ratings = dataset.map(review => (review.user, review.rating))

    // example 4.8 https://www.oreilly.com/library/view/learning-spark/9781449359034/ch04.html
    val avg_user_rating_map = just_ratings
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(sum_count => 
        if (sum_count._2 != 0)
          sum_count._1 / sum_count._2
        else 
          glob_avg
      ).collect().toMap
    // key = user, value = (ratings sum, # of ratings)

    val norm_devs = dataset
      .filter(_.item == itemId)
      .map(
        review => normalizedDev(review, avg_user_rating_map(review.user))
      )
    return norm_devs.mean
  }

  def distr_baselineRating(dataset: DistrRatingArr, userId: Int, itemId: Int): Double = {
    val user_avg = distr_userAvgRating(dataset, userId)
    val mean_item_dev = distr_itemAvgDev(dataset, itemId)
    return baselinePrediction(user_avg, mean_item_dev)
  }

  def distr_baselineRatingMAE(train_dataset: DistrRatingArr, 
                              test_dataset : DistrRatingArr): Double = {
    val glob_avg = distr_globalAvgRating(train_dataset)

    val user_avg_map = train_dataset
      .map(review => (review.user, review.rating))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(sum_count => sum_count._1 / sum_count._2).collect()
      .toMap
      .withDefaultValue(glob_avg)

    val item_dev_map = train_dataset
      // compute normal deviation for each review
      .map(
        review => (review.item, normalizedDev(review, user_avg_map(review.user)))
      )
      // count # of occurances of each item, sum normal devs for each item
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      // compute mean
      .mapValues(sum_count => 
        sum_count._1 / sum_count._2
      ).collect()
      .toMap
      // if element is not in the train dataset
      .withDefaultValue(glob_avg)

    val err_base_avg = test_dataset.map(review => 
      getMAE(review.rating, baselinePrediction(user_avg_map(review.user), item_dev_map(review.item)))
    )

    return err_base_avg.mean
  }

 

  // ===== Personalized prediction =====

  def similarityUniform(user_aaa: Int, user_bbb: Int, dataset: RatingArr): Double = {
    1.0
  }

  def similarityCosine(user_aaa: Int, user_bbb: Int, preproc_dataset: RatingArr): Double = {    
    val aaa_reviews = preproc_dataset.filter(_.user == user_aaa)
    val bbb_reviews = preproc_dataset.filter(_.user == user_bbb)
    val aaa_items = aaa_reviews.map(_.item)
    val bbb_items = bbb_reviews.map(_.item)
    val intersect_items = aaa_items.intersect(bbb_items)
    val vect_aaa = aaa_reviews.filter(rev => intersect_items.contains(rev.item)).sortBy(_.item).map(_.rating)
    val vect_bbb = bbb_reviews.filter(rev => intersect_items.contains(rev.item)).sortBy(_.item).map(_.rating)

    return (vect_aaa, vect_bbb).zipped.map(_*_).sum
  }

  def similarityJaccard(user_aaa: Int, user_bbb: Int, dataset: RatingArr): Double = {
    val aaa_items = dataset.filter(_.user == user_aaa).map(_.item)
    val bbb_items = dataset.filter(_.user == user_bbb).map(_.item)

    val intersect_items = aaa_items.intersect(bbb_items)
    val intersect_count = intersect_items.size
    val union_count = aaa_items.size + bbb_items.size - intersect_count

    if (union_count == 0)
      0.0
    else 
      intersect_count.toDouble / union_count.toDouble
  }

  def justSimilarityCosine(user1: Int, user2: Int, dataset: RatingArr): Double = {
    val preproc_arr = preprocDatasetOld(dataset, userAvgMap(dataset))
    return similarityCosine(user1, user2, preproc_arr)
  }


  def itemWeightedDev(dataset: RatingArr, preproc_arr: RatingArr, itemId: Int, userId: Int, 
                      similarity: SimilarityFunc, user_avg_map: Map[Int,Double]): Double = {
    val item_reviews = dataset.filter(_.item == itemId)
  
    if (item_reviews.isEmpty)
      return 0.0

    val weighted_devs = item_reviews.map(
      review => normalizedDev(review, user_avg_map(review.user)) * similarity(userId, review.user, preproc_arr)
    ).sum
    val norm_coef = item_reviews.map(
      review => scala.math.abs(similarity(userId, review.user, preproc_arr))
    ).sum

    return weighted_devs / norm_coef
  }

  def personalizedRatingUniform(dataset: RatingArr, userId: Int, itemId: Int): Double = {
    val user_avg_map = userAvgMap(dataset)
    val weighted_item_dev = itemWeightedDev(dataset, dataset, itemId, userId, similarityUniform, user_avg_map)

    return baselinePrediction(user_avg_map(userId), weighted_item_dev)
  }

  def personalizedRatingCosine (dataset: RatingArr, userId: Int, itemId: Int): Double = {
    // we could compute it only for one user - would be quicker
    val user_avg_map = userAvgMap(dataset)
    val preproc_arr = preprocDatasetOld(dataset, user_avg_map)
    val weighted_item_dev = itemWeightedDev(dataset, preproc_arr, itemId, userId, similarityCosine, user_avg_map)

    return baselinePrediction(user_avg_map(userId), weighted_item_dev)
  }

  def personalizedRatingJaccard(dataset: RatingArr, userId: Int, itemId: Int): Double = {
    val user_avg_map = userAvgMap(dataset)
    val weighted_item_dev = itemWeightedDev(dataset, dataset, itemId, userId, similarityJaccard, user_avg_map)

    return baselinePrediction(user_avg_map(userId), weighted_item_dev)
  }


  def personalizedUniformMAE(train_dataset: RatingArr, 
                             test_dataset : RatingArr): Double = {
    val user_avg_map = userAvgMap(train_dataset)

    /* could be optimized */
    val err_base_avg = test_dataset.map(rev => getMAE(rev.rating,
        baselinePrediction(user_avg_map(rev.user), 
                           itemWeightedDev(train_dataset, train_dataset, rev.item, rev.user, similarityUniform, user_avg_map))
      )
    )
    
    return mean(err_base_avg)
  }


  // cosine personalized
  def sqDev(review: Rating, usr_avg: Double): Double = {
    val dev = normalizedDev(review, usr_avg)
    return dev * dev
  }

  // 2 sec quicker but incorrect
  def preprocDataset(dataset: RatingArr, avg_user_map: Map[Int, Double]): RatingArr = {
    val denom_map = dataset
      // squared normalized deviation for each rated item
      .map(review => (review.user, sqDev(review, avg_user_map(review.user))))
      .toMap
      // group by user
      .groupBy(_._1)
      // for each user sum the (squared normalized deviation) of items they rated
      .mapValues(_.map(_._2).sum)
      // take a square root - get the denominator of the preprocessed rating
      .mapValues(scala.math.sqrt(_))
      // make a map to access the denominator for each user
      .toMap
      
    val preproc_dataset = dataset.map(review => Rating(review.user, review.item, 
        normalizedDev(review, avg_user_map(review.user)) / denom_map(review.user)
      )
    )

    return preproc_dataset
  }

  def preprocDatasetOld(dataset: RatingArr, avg_user_map: Map[Int,Double]): RatingArr = {
    val users = dataset.map(_.user).distinct
    val denom_map = users.map(user_id => (user_id, 
        scala.math.sqrt(
          dataset.filter(rev => rev.user == user_id).map(rev => sqDev(rev, avg_user_map(rev.user))).sum
        )
      )
    ).toMap

    val preproc_dataset = dataset.map(
      review => Rating(review.user, review.item, 
                       normalizedDev(review, avg_user_map(review.user)) / denom_map(review.user))
    )

    return preproc_dataset
  }


  def orderPair(num1: Int, num2: Int): (Int, Int) = {
    if (num1 <= num2) 
      (num1, num2)
    else
      (num2, num1)
  } 

  def cross[A, B](a: Iterable[A], b: Iterable[B]): Iterable[(A, B)] = {
    for (i <- a; j <- b) yield (i, j)
  }

  def personalizedComplexMAE(train_dataset: RatingArr, 
                             test_dataset : RatingArr,
                             sim_func     : SimilarityFunc): Double = {
    val user_avg_map = userAvgMap(train_dataset)
    val preproc_arr = preprocDatasetOld(train_dataset, user_avg_map)

    val similarity_map = scala.collection.mutable.Map.empty[(Int,Int),Double]

    def quickSim(user1: Int, user2: Int): Double = {
      // always order user1 <= user2 since sim(u,v)=sim(v,u)
      val usrs = orderPair(user1, user2)

      // compute or get (if already computed) the similarity
      return similarity_map.getOrElseUpdate(
        usrs, sim_func(usrs._1, usrs._2, preproc_arr)
      )
    }

    def quickItemDev(itemId: Int, userId: Int): Double = {
      val item_reviews = train_dataset.filter(_.item == itemId)
    
      if (item_reviews.isEmpty)
        return 0.0

      // pair of weighted deviations (nominator) and normalization coefficients (denominator)
      val item_dev = item_reviews
        .map(review => {
          val sims = quickSim(userId, review.user)
          val weighted_dev = normalizedDev(review, user_avg_map(review.user)) * sims
          (weighted_dev, scala.math.abs(sims))
        })
        .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

      return item_dev._1 / item_dev._2
    }

    val err_base_avg = test_dataset.map(rev => getMAE(rev.rating,
        baselinePrediction(user_avg_map(rev.user), quickItemDev(rev.item, rev.user))
      )
    )

    return mean(err_base_avg)
  }
}    
