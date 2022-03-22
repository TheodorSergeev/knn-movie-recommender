package shared

package object predictions
{
  case class Rating(user: Int, item: Int, rating: Double)


  // default code
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


  // custom timing functions
 
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


  // custom types
  type RatingArr = Array[shared.predictions.Rating]
  type DistrRatingArr = org.apache.spark.rdd.RDD[Rating]


  // baseline prediction

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

  def scaleRatingToUserAverage(rating: Double, avgRating: Double): Double = {
    if (rating > avgRating)
      5.0 - avgRating
    else if (rating < avgRating)
      avgRating - 1.0
    else
      1.0
  }

  def normalizedDev(review: Rating, avg_user_rating: Double): Double = {
    return (review.rating - avg_user_rating) / scaleRatingToUserAverage(review.rating, avg_user_rating)
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
    return user_avg + item_dev * scaleRatingToUserAverage(user_avg + item_dev, user_avg)
  }

  def baselineRating(dataset: RatingArr, userId: Int, itemId: Int): Double = {
    val user_avg = userAvgRating(dataset, userId)
    val mean_item_dev = itemAvgDev(dataset, itemId)
    return baselinePrediction(user_avg, mean_item_dev)
  }

  def getMAE(val1: Double, val2: Double): Double = {
    scala.math.abs(val1 - val2)
  }

  def globalAvgRatingMAE(train_dataset: RatingArr, 
                         test_dataset : RatingArr): Double = {
    //val func = dataset, review => Double ?
    val glob_avg = globalAvgRating(train_dataset)
    val err_glob_avg = test_dataset.map(review => getMAE(review.rating, glob_avg))

    return mean(err_glob_avg)
  }

  def userAvgRatingMAE(train_dataset: RatingArr, 
                       test_dataset : RatingArr): Double = {
    val all_users = (train_dataset ++ test_dataset).map(_.user).distinct
    val user_avg_map = all_users.map(user => (user, userAvgRating(train_dataset, user))).toMap

    val err_user_avg = test_dataset.map(review => getMAE(review.rating, user_avg_map(review.user)))
    return mean(err_user_avg)
  }

  def itemAvgRatingMAE(train_dataset: RatingArr, 
                       test_dataset : RatingArr): Double = {
    val all_items = (train_dataset ++ test_dataset).map(_.item).distinct
    val item_avg_map = all_items.map(item => (item, itemAvgRating(train_dataset, item))).toMap

    val err_item_avg = test_dataset.map(review => getMAE(review.rating, item_avg_map(review.item)))
    return mean(err_item_avg)
  }

  def baselineRatingMAE(train_dataset: RatingArr, 
                        test_dataset : RatingArr): Double = {
    val all_users = (train_dataset ++ test_dataset).map(_.user).distinct
    val user_avg_map = all_users.map(user => (user, userAvgRating(train_dataset, user))).toMap


    val all_items =  (train_dataset ++ test_dataset).map(_.item).distinct
    /*could be optimized*/
    val item_dev_map = all_items.map(item => (item, itemAvgDev(train_dataset, item))).toMap

    val err_base_avg = test_dataset.map(review => 
      getMAE(review.rating, baselinePrediction(user_avg_map(review.user), item_dev_map(review.item)))
    )

    return mean(err_base_avg)
  }


  // distributed prediction

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
      .mapValues(sum_count => 
        sum_count._1 / sum_count._2
      ).collect()
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

 
  // personalized prediction
  type SimilarityFunc = (Int, Int, RatingArr) => Double

  def sumSeq(arr: Seq[Double]): Double = arr.reduce(_+_)

  def similarityUniform(user_aaa: Int, user_bbb: Int, dataset: RatingArr): Double = 1.0

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
    val user_avg = userAvgRating(dataset, userId)

    val all_users = dataset.map(_.user).distinct
    val user_avg_map = all_users.map(user => (user, userAvgRating(dataset, user))).toMap

    val weighted_item_dev = itemWeightedDev(dataset, dataset, itemId, userId, similarityUniform, user_avg_map)

    return baselinePrediction(user_avg, weighted_item_dev)
  }

  def personalizedUniformMAE(train_dataset: RatingArr, 
                             test_dataset : RatingArr): Double = {

    val all_users = (train_dataset ++ test_dataset).map(_.user).distinct
    val user_avg_map = all_users.map(user => (user, userAvgRating(train_dataset, user))).toMap

    /* could be optimized */
    val err_base_avg = test_dataset.map(review => getMAE(review.rating,
        baselinePrediction(user_avg_map(review.user), 
                           itemWeightedDev(train_dataset, train_dataset, review.item, review.user, similarityUniform, user_avg_map))
      )
    )
    
    return mean(err_base_avg)
  }

  def sqDev(review: Rating, usr_avg: Double): Double = {
    val dev = normalizedDev(review, usr_avg)
    return dev * dev
  }

  def preprocDataset(dataset: RatingArr, avg_user_map: Map[Int,Double]): RatingArr = {
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

  def similarityCosine(user_aaa: Int, user_bbb: Int, preproc_dataset: RatingArr): Double = {
    val aaa_reviews = preproc_dataset.filter(_.user == user_aaa)
    val bbb_reviews = preproc_dataset.filter(_.user == user_bbb)
    val aaa_items = aaa_reviews.map(_.item)
    val bbb_items = bbb_reviews.map(_.item)
    val intersect_items = aaa_items.intersect(bbb_items)

    /* could be improved */
    val vect_aaa = aaa_reviews.filter(rev => intersect_items.contains(rev.item)).sortBy(_.item).map(_.rating)
    val vect_bbb = bbb_reviews.filter(rev => intersect_items.contains(rev.item)).sortBy(_.item).map(_.rating)

    return (vect_aaa, vect_bbb).zipped.map(_*_).sum
  }

  def personalizedRatingCosine(dataset: RatingArr, userId: Int, itemId: Int): Double = {
    //val user_avg = userAvgRating(dataset, userId)
    val user_avg_map = dataset.map(rev => (rev.user, userAvgRating(dataset, rev.user))).toMap

    val preproc_arr = preprocDataset(dataset, user_avg_map)

    val weighted_item_dev = itemWeightedDev(dataset, preproc_arr, itemId, userId, similarityCosine, user_avg_map)

    return baselinePrediction(user_avg_map(userId), weighted_item_dev)
  }

  def justSimilarityCosine(user1: Int, user2: Int, dataset: RatingArr): Double = {
    val user_avg_map = dataset.map(rev => (rev.user, userAvgRating(dataset, rev.user))).toMap
    val preproc_arr = preprocDataset(dataset, user_avg_map)
    return similarityCosine(user1, user2, preproc_arr)
  }

  def itemWeightedDev3(dataset       : RatingArr, 
                       preproc_arr   : RatingArr, 
                       itemId        : Int, 
                       userId        : Int, 
                       similarity_arr: Map[Int, Map[Int, Double]], 
                       user_avg_map  : Map[Int,Double]            ): Double = {
    val item_reviews = dataset.filter(_.item == itemId)
  
    if (item_reviews.isEmpty)
      return 0.0

    val weighted_devs = item_reviews.map(review => 
      if (userId < review.user) 
        normalizedDev(review, user_avg_map(review.user)) * similarity_arr(userId)(review.user)
      else
        normalizedDev(review, user_avg_map(review.user)) * similarity_arr(review.user)(userId)
    ).sum
    val norm_coef = item_reviews.map(
      review => scala.math.abs(similarity_arr(userId)(review.user))
    ).sum

    return weighted_devs / norm_coef
  }

  def personalizedCosineMAE(train_dataset: RatingArr, 
                            test_dataset : RatingArr): Double = {

    val all_users = (train_dataset ++ test_dataset).map(_.user).distinct
    val user_avg_map = all_users.map(user => (user, userAvgRating(train_dataset, user))).toMap
    val preproc_arr = preprocDataset(train_dataset, user_avg_map)

    println("asd1")

    // similarity(u,v) = similarity(v,u) !
    // so this can be optimized
    val similarity_arr = all_users.map(user1 => 
      (user1, all_users.map(user2 => (user2, similarityCosine(user1, user2, preproc_arr))).toMap)
    ).toMap

    println("asd2")

    /* could be optimized */
    val err_base_avg = test_dataset.map(review => getMAE(review.rating,
        baselinePrediction(user_avg_map(review.user), 
                           itemWeightedDev3(train_dataset, preproc_arr, review.item, review.user, similarity_arr, user_avg_map))
      )
    )

    println("asd3")

    return mean(err_base_avg)
  }


  def jaccardCoef(user_aaa: Int, user_bbb: Int, dataset: RatingArr): Double = {
    val aaa_items = Set(dataset.filter(_.user == user_aaa).map(_.item))
    val bbb_items = Set(dataset.filter(_.user == user_bbb).map(_.item))
    val intersect_items = aaa_items.intersect(bbb_items)
    val intersect_count = intersect_items.size
    val union_count = aaa_items.size + bbb_items.size - intersect_count

    if (union_count == 0)
      0.0
    else 
      intersect_count.toDouble / union_count.toDouble
  }

  def personalizedRatingJaccard(dataset: RatingArr, userId: Int, itemId: Int): Double = {
    //val user_avg = userAvgRating(dataset, userId)
    val user_avg_map = dataset.map(rev => (rev.user, userAvgRating(dataset, rev.user))).toMap
    val weighted_item_dev = itemWeightedDev2(dataset, dataset, itemId, userId, jaccardCoef, user_avg_map)

    return baselinePrediction(user_avg_map(userId), weighted_item_dev)
  }

  def personalizedJaccardMAE(train_dataset: RatingArr, 
                             test_dataset : RatingArr): Double = {

    val all_users = (train_dataset ++ test_dataset).map(_.user).distinct
    val user_avg_map = all_users.map(user => (user, userAvgRating(train_dataset, user))).toMap

    val similarity_arr = all_users.map(user1 => (user1, 
        all_users.filter(user2 => user2 >= user1)
                 .map(user2 => (user2, jaccardCoef(user1, user2, train_dataset))
      ).toMap)
    ).toMap

    /* could be optimized */
    val err_base_avg = test_dataset.map(review => getMAE(review.rating,
        baselinePrediction(user_avg_map(review.user), 
                           itemWeightedDev3(train_dataset, train_dataset, review.item, review.user, similarity_arr, user_avg_map))
      )
    )

    return mean(err_base_avg)
  }
}
