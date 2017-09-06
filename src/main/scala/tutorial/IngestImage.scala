package tutorial

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.raster.split.Split
import geotrellis.proj4._

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._

import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd._

import scala.io.StdIn
import java.io.File

object IngestImage {
  val inputPath = "file://" + new File("data").getAbsolutePath + "/r-g-nir-*.tif"
  val outputPath = "data/catalog"
  def main(args: Array[String]): Unit = {
    // Setup Spark to use Kryo serializer.
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Spark Tiler")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

    val sc = new SparkContext(conf)
    try {
      run(sc)
      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }
  }

  def fullPath(path: String) = new java.io.File(path).getAbsolutePath

  /** Calculate the layer metadata for the incoming landsat images
   *
   * Normally we would have no information about the incoming rasters and we be forced
   * to use [[TileLayerMetadata.fromRdd]] to collect it before we could tile the imagery.
   * But in this case the pre-query from scala-landsatutil is providing enough
   * information that the metadata can be calculated.
   *
   * Collecting metadata before tiling step requires either reading the data twice
   * or caching records in spark memory. In either case avoiding collection is a performance boost.
   */
  def calculateTileLayerMetadata(maxZoom: Int = 13, destCRS: CRS = WebMercator) = {
    val layoutDefinition = ZoomedLayoutScheme.layoutForZoom(maxZoom, destCRS.worldExtent, 256)
    // simplify and pretend like we have tiles to cover the world
    //instead of: val imageExtent = images.map(_.footprint.envelope).reduce(_ combine _).reproject(LatLng, destCRS)
    val imageExtent = WebMercator.worldExtent

    val GridBounds(colMin, rowMin, colMax, rowMax) = layoutDefinition.mapTransform(imageExtent)
    TileLayerMetadata(
      cellType = UShortCellType,
      layout = layoutDefinition,
      extent = imageExtent,
      crs = destCRS,
      bounds = KeyBounds(
        SpatialKey(colMin, rowMin),
        SpatialKey(colMax, rowMax))
    )
  }

  def run(implicit sc: SparkContext) = {
    // Read the geotiff in as a single image RDD,
    // using a method implicitly added to SparkContext by
    // an implicit class available via the
    // "import geotrellis.spark.io.hadoop._ " statement.
    val reprojected: RDD[(ProjectedExtent, MultibandTile)] =
      HadoopGeoTiffRDD.spatialMultiband(
        inputPath, HadoopGeoTiffRDD.Options(maxTileSize=Some(512), numPartitions=Some(8))
      ).map { case (ProjectedExtent(extent, crs), scene) =>
          val reprojected = Raster(scene, extent).reproject(crs, WebMercator)
          ProjectedExtent(reprojected.extent, WebMercator) -> reprojected.tile
      }


    // Avoid use the "TileLayerMetadata.fromRdd" to save an iteration over the files
    // Since we know our input is landsat we can construct appropriate layer metadata
    val baseZoom = 13
    val rasterMetaData = calculateTileLayerMetadata(baseZoom, WebMercator)

    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
    // We'll repartition it so that there are more partitions to work with, since spark
    // likes to work with more, smaller partitions (to a point) over few and large partitions.
    val tiled: RDD[(SpatialKey, MultibandTile)] =
      reprojected
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)

    val baseLayer: MultibandTileLayerRDD[SpatialKey] = ContextRDD(tiled, rasterMetaData)

    // We'll be tiling the images using a zoomed layout scheme
    // in the web mercator format (which fits the slippy map tile specification).
    // We'll be creating 256 x 256 tiles.
    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

    // Create the attributes store that will tell us information about our catalog.
    val attributeStore = FileAttributeStore(outputPath)

    // Create the writer that we will use to store the tiles in the local catalog.
    val writer = FileLayerWriter(attributeStore)

    // Pyramiding up the zoom levels, write our tiles out to the local file system.
    Pyramid.upLevels(baseLayer, layoutScheme, baseZoom, Bilinear) { (rdd, z) =>
      val layerId = LayerId("landsat", z)
      // If the layer exists already, delete it out before writing
      if(attributeStore.layerExists(layerId)) {
        new FileLayerManager(attributeStore).delete(layerId)
      }
      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
    }
  }
}
