# coding=utf-8

import unicodedata
from itertools import chain

import arrow
import requests
from rasterio import warp

from marblecutter import NoDataAvailable, get_source, get_zoom
from marblecutter.catalogs import WGS84_CRS, Catalog


class OAMSceneCatalog(Catalog):
    def __init__(self, uri):
        scene = requests.get(uri).json()

        self._bounds = scene['bounds']
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
    def headers(self):
        return {}

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
        rsp = requests.get(uri)

        if not rsp.ok:
            raise NoDataAvailable()

        oin_meta = rsp.json()
        self._meta = oin_meta
        self._metadata_url = uri
        self._name = oin_meta.get('title')
        self._provider = oin_meta.get('provider')
        self._resolution = oin_meta.get('gsd')
        self._source = oin_meta.get('uuid')

        with get_source(self._source) as source:
            self._bounds = warp.transform_bounds(source.crs, WGS84_CRS,
                                                 *source.bounds)

        approximate_zoom = get_zoom(self._resolution)
        self._center = [(self._bounds[0] + self.bounds[2]) / 2,
                        (self._bounds[1] + self.bounds[3]) / 2,
                        approximate_zoom - 3]
        self._maxzoom = approximate_zoom + 3
        self._minzoom = approximate_zoom - 10

    def get_sources(self, (bounds, bounds_crs), resolution):
        left, bottom, right, top = warp.transform_bounds(
            bounds_crs, WGS84_CRS, *bounds)

        if (self._bounds[0] <= left <= self._bounds[2]
                or self._bounds[0] <= right <= self._bounds[2]) and (
                    self._bounds[1] <= bottom <= self._bounds[3]
                    or self._bounds[1] <= top <= self._bounds[3]):
            return [(self._source, self._name, self._resolution)]

        return []

    @property
    def bounds(self):
        return self._bounds

    @property
    def center(self):
        return self._center

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
