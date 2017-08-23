# coding=utf-8

from itertools import chain

from rasterio import warp

import requests
from marblecutter import get_zoom
from marblecutter.catalogs import WGS84_CRS, Catalog


class OAMSceneCatalog(Catalog):
    def __init__(self, uri):
        scene = requests.get(uri).json()

        self._center = scene['center']
        self._maxzoom = scene['maxzoom']
        self._minzoom = scene['minzoom']
        self._name = scene['name']

        self._sources = [
            OINMetaCatalog(
                source['meta']['source'].replace('_warped.vrt', '_meta.json'))
            for source in reversed(scene['meta']['sources'])
        ]

    def get_sources(self, (bounds, bounds_crs), resolution):
        return chain(*[
            s.get_sources((bounds, bounds_crs), resolution)
            for s in self._sources
        ])

    @property
    def bounds(self):
        return self._bounds

    @property
    def center(self):
        return self._center

    @property
    def maxzoom(self):
        return self._maxzoom

    @property
    def minzoom(self):
        return self._minzoom

    @property
    def name(self):
        return self._name


class OINMetaCatalog(Catalog):
    def __init__(self, uri):
        oin_meta = requests.get(uri).json()

        self._bounds = oin_meta['bbox']
        self._metadata_url = uri
        self._name = oin_meta['title']
        self._provider = oin_meta['provider']
        self._resolution = oin_meta['gsd']
        self._source = oin_meta['uuid']

        approximate_zoom = get_zoom(self._resolution)
        self._center = [
            (self._bounds[0] + self.bounds[2]) / 2,
            (self._bounds[1] + self.bounds[3]) / 2,
            approximate_zoom - 3
        ]
        self._maxzoom = approximate_zoom + 3
        self._minzoom = approximate_zoom - 10

    def get_sources(self, (bounds, bounds_crs), resolution):
        ((left, right), (bottom, top)) = warp.transform(
            bounds_crs, WGS84_CRS, bounds[::2], bounds[1::2])

        if (
            self._bounds[0] <= left <= self._bounds[2] or
            self._bounds[0] <= right <= self._bounds[2]
        ) and (
            self._bounds[1] <= bottom <= self._bounds[3] or
            self._bounds[1] <= top <= self._bounds[3]
        ):
            return [(self._source, self._name, self._resolution)]

        return []

    @property
    def bounds(self):
        return self._bounds

    @property
    def center(self):
        return self._center

    @property
    def id(self):
        return self._name

    @property
    def maxzoom(self):
        return self._maxzoom

    @property
    def metadata_url(self):
        return self._metadata_url

    @property
    def minzoom(self):
        return self._minzoom

    @property
    def name(self):
        return self._name

    @property
    def provider(self):
        return self._provider
