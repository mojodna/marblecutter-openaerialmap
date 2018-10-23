# coding=utf-8

import json
import logging
import math
import multiprocessing
import os
import unicodedata
from concurrent import futures
from distutils.util import strtobool
from itertools import chain

import boto3
import requests
from boto3.session import Config

import arrow
from marblecutter import (
    Bounds,
    NoCatalogAvailable,
    get_resolution_in_meters,
    get_source,
    get_zoom,
)
from marblecutter.catalogs import WGS84_CRS, Catalog
from marblecutter.utils import Source
from rasterio import warp

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

LOG = logging.getLogger(__name__)

# GDAL-compatible environment variables
AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT")
AWS_HTTPS = bool(strtobool(os.getenv("AWS_HTTPS", "YES")))
AWS_VIRTUAL_HOSTING = bool(strtobool(os.getenv("AWS_VIRTUAL_HOSTING", "YES")))

if AWS_HTTPS and AWS_S3_ENDPOINT is not None:
    endpoint_url = "https://" + AWS_S3_ENDPOINT
elif AWS_S3_ENDPOINT is not None:
    endpoint_url = "http://" + AWS_S3_ENDPOINT
else:
    endpoint_url = None

if AWS_VIRTUAL_HOSTING:
    config = Config(s3={"addressing_style": "virtual"})
else:
    # disable virtual hosting (<bucket>.endpoint_url)
    config = Config(s3={"addressing_style": "path"})

S3 = boto3.client("s3", endpoint_url=endpoint_url, config=config)


class OAMSceneCatalog(Catalog):

    def __init__(self, uri):
        if uri.startswith("s3://"):
            url = urlparse(uri)
            obj = S3.get_object(Bucket=url.netloc, Key=url.path[1:])
            scene = json.loads(obj["Body"].read().decode("utf-8"))
        elif uri.startswith(("http://", "https://")):
            scene = requests.get(uri).json()
        else:
            raise NoCatalogAvailable()

        self._bounds = scene["bounds"]
        self._center = scene["center"]
        self._maxzoom = scene["maxzoom"]
        self._minzoom = scene["minzoom"]
        self._name = scene["name"]

        def _build_catalog(source):
            return OINMetaCatalog(
                source["meta"]["source"].replace("_warped.vrt", "_meta.json")
            )

        sources = list(reversed(scene["meta"]["sources"]))
        with futures.ThreadPoolExecutor(
            max_workers=multiprocessing.cpu_count() * 5
        ) as executor:
            self._sources = list(executor.map(_build_catalog, sources))

    def get_sources(self, bounds, resolution):
        return chain(*[s.get_sources(bounds, resolution) for s in self._sources])


class OINMetaCatalog(Catalog):

    def __init__(self, uri):
        try:
            if uri.startswith("s3://"):
                url = urlparse(uri)
                obj = S3.get_object(Bucket=url.netloc, Key=url.path[1:])
                oin_meta = json.loads(obj["Body"].read().decode("utf-8"))
            elif uri.startswith(("http://", "https://")):
                oin_meta = requests.get(uri).json()
            else:
                raise NoCatalogAvailable()
        except Exception:
            raise NoCatalogAvailable()

        self._meta = oin_meta
        self._metadata_url = uri
        self._name = oin_meta.get("title")
        self._provider = oin_meta.get("provider")
        self._source = oin_meta.get("uuid")

        with get_source(self._source) as src:
            self._bounds = warp.transform_bounds(src.crs, WGS84_CRS, *src.bounds)
            self._resolution = get_resolution_in_meters(
                Bounds(src.bounds, src.crs), (src.height, src.width)
            )
            approximate_zoom = get_zoom(max(self._resolution), op=math.ceil)

            if src.meta["dtype"] != "uint8":
                global_min = src.get_tag_item("TIFFTAG_MINSAMPLEVALUE")
                global_max = src.get_tag_item("TIFFTAG_MAXSAMPLEVALUE")

                for band in range(0, src.count):
                    self._meta["values"] = self._meta.get("values", {})
                    self._meta["values"][band] = {}
                    min_val = src.get_tag_item("STATISTICS_MINIMUM", bidx=band + 1)
                    max_val = src.get_tag_item("STATISTICS_MAXIMUM", bidx=band + 1)
                    mean_val = src.get_tag_item("STATISTICS_MEAN", bidx=band + 1)

                    if min_val is not None:
                        self._meta["values"][band]["min"] = float(min_val)
                    elif global_min is not None:
                        self._meta["values"][band]["min"] = float(global_min)

                    if max_val is not None:
                        self._meta["values"][band]["max"] = float(max_val)
                    elif global_max is not None:
                        self._meta["values"][band]["max"] = float(global_max)

                    if mean_val is not None:
                        self._meta["values"][band]["mean"] = float(mean_val)

        self._center = [
            (self._bounds[0] + self.bounds[2]) / 2,
            (self._bounds[1] + self.bounds[3]) / 2,
            approximate_zoom - 3,
        ]
        self._maxzoom = approximate_zoom + 3
        self._minzoom = approximate_zoom - 10

    def get_sources(self, bounds, resolution):
        bounds, bounds_crs = bounds
        zoom = get_zoom(max(resolution))
        left, bottom, right, top = warp.transform_bounds(bounds_crs, WGS84_CRS, *bounds)

        if (
            (
                self._bounds[0] <= left <= self._bounds[2]
                or self._bounds[0] <= right <= self._bounds[2]
            )
            and (
                self._bounds[1] <= bottom <= self._bounds[3]
                or self._bounds[1] <= top <= self._bounds[3]
            )
            and (self._minzoom <= zoom <= self._maxzoom)
        ):
            yield Source(
                self._source,
                self._name,
                self._resolution,
                {},
                self._meta,
                {"imagery": True},
            )

    @property
    def headers(self):
        headers = {"X-OIN-Metadata-URL": self._metadata_url}

        if "acquisition_start" in self._meta or "acquisition_end" in self._meta:
            start = self._meta.get("acquisition_start")
            end = self._meta.get("acquisition_end")

            if start and end:
                start = arrow.get(start)
                end = arrow.get(end)

                capture_range = "{}-{}".format(
                    start.format("M/D/YYYY"), end.format("M/D/YYYY")
                )
                headers["X-OIN-Acquisition-Start"] = start.format(
                    "YYYY-MM-DDTHH:mm:ssZZ"
                )
                headers["X-OIN-Acquisition-End"] = end.format("YYYY-MM-DDTHH:mm:ssZZ")
            elif start:
                start = arrow.get(start)

                capture_range = start.format("M/D/YYYY")
                headers["X-OIN-Acquisition-Start"] = start.format(
                    "YYYY-MM-DDTHH:mm:ssZZ"
                )
            elif end:
                end = arrow.get(end)

                capture_range = end.format("M/D/YYYY")
                headers["X-OIN-Acquisition-End"] = end.format("YYYY-MM-DDTHH:mm:ssZZ")

            # Bing Maps-compatibility (JOSM uses this)
            headers["X-VE-TILEMETA-CaptureDatesRange"] = capture_range

        if "provider" in self._meta:
            headers["X-OIN-Provider"] = unicodedata.normalize(
                "NFKD", self._meta["provider"]
            ).encode(
                "ascii", "ignore"
            )

        if "platform" in self._meta:
            headers["X-OIN-Platform"] = unicodedata.normalize(
                "NFKD", self._meta["platform"]
            ).encode(
                "ascii", "ignore"
            )

        return headers
