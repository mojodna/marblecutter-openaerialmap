# coding=utf-8

import math
import multiprocessing
import unicodedata
from concurrent import futures
from itertools import chain

import requests

import arrow
from marblecutter import (Bounds, NoDataAvailable, get_resolution_in_meters,
                          get_source, get_zoom)
from marblecutter.catalogs import WGS84_CRS, Catalog
from marblecutter.utils import Source
from rasterio import warp


class OAMSceneCatalog(Catalog):
    def __init__(self, uri):
        scene = requests.get(uri).json()

        self._bounds = scene['bounds']
        self._center = scene['center']
        self._maxzoom = scene['maxzoom']
        self._minzoom = scene['minzoom']
        self._name = scene['name']

        def _build_catalog(source):
            return OINMetaCatalog(
                source['meta']['source'].replace('_warped.vrt', '_meta.json'))

        sources = list(reversed(scene['meta']['sources']))
        with futures.ThreadPoolExecutor(
                max_workers=multiprocessing.cpu_count() * 5) as executor:
            self._sources = list(executor.map(_build_catalog, sources))

    def get_sources(self, (bounds, bounds_crs), resolution):
        return chain(* [
            s.get_sources((bounds, bounds_crs), resolution)
            for s in self._sources
        ])


class OINMetaCatalog(Catalog):
    def __init__(self, uri):
        rsp = requests.get(uri)

        if not rsp.ok:
            raise NoDataAvailable()

        oin_meta = rsp.json()
        self._meta = oin_meta
        self._metadata_url = uri
        self._name = oin_meta.get('title')
        self._provider = oin_meta.get('provider')
        self._source = oin_meta.get('uuid')

        with get_source(self._source) as src:
            self._bounds = warp.transform_bounds(src.crs, WGS84_CRS,
                                                 *src.bounds)
            self._resolution = get_resolution_in_meters(
                Bounds(src.bounds, src.crs), (src.height, src.width))
            approximate_zoom = get_zoom(max(self._resolution), op=math.ceil)

        self._center = [(self._bounds[0] + self.bounds[2]) / 2,
                        (self._bounds[1] + self.bounds[3]) / 2,
                        approximate_zoom - 3]
        self._maxzoom = approximate_zoom + 3
        self._minzoom = approximate_zoom - 10

    def get_sources(self, bounds, resolution):
        bounds, bounds_crs = bounds
        zoom = get_zoom(max(resolution))
        left, bottom, right, top = warp.transform_bounds(
            bounds_crs, WGS84_CRS, *bounds)

        if (self._bounds[0] <= left <= self._bounds[2]
                or self._bounds[0] <= right <= self._bounds[2]) and (
                    self._bounds[1] <= bottom <= self._bounds[3]
                    or self._bounds[1] <= top <= self._bounds[3]) and (
                        self._minzoom <= zoom <= self._maxzoom):
            yield Source(self._source, self._name, self._resolution, {}, {},
                         {"imagery": True})

    @property
    def headers(self):
        headers = {
            'X-OIN-Metadata-URL': self._metadata_url,
        }

        if ('acquisition_start' in self._meta
                or 'acquisition_end' in self._meta):
            start = self._meta.get('acquisition_start')
            end = self._meta.get('acquisition_end')

            if start and end:
                start = arrow.get(start)
                end = arrow.get(end)

                capture_range = '{}-{}'.format(
                    start.format('M/D/YYYY'), end.format('M/D/YYYY'))
                headers['X-OIN-Acquisition-Start'] = start.format(
                    'YYYY-MM-DDTHH:mm:ssZZ')
                headers['X-OIN-Acquisition-End'] = end.format(
                    'YYYY-MM-DDTHH:mm:ssZZ')
            elif start:
                start = arrow.get(start)

                capture_range = start.format('M/D/YYYY')
                headers['X-OIN-Acquisition-Start'] = start.format(
                    'YYYY-MM-DDTHH:mm:ssZZ')
            elif end:
                end = arrow.get(end)

                capture_range = end.format('M/D/YYYY')
                headers['X-OIN-Acquisition-End'] = end.format(
                    'YYYY-MM-DDTHH:mm:ssZZ')

            # Bing Maps-compatibility (JOSM uses this)
            headers['X-VE-TILEMETA-CaptureDatesRange'] = capture_range

        if 'provider' in self._meta:
            headers['X-OIN-Provider'] = unicodedata.normalize(
                'NFKD', self._meta['provider']).encode('ascii', 'ignore')

        if 'platform' in self._meta:
            headers['X-OIN-Platform'] = unicodedata.normalize(
                'NFKD', self._meta['platform']).encode('ascii', 'ignore')

        return headers
