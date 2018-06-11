# coding=utf-8
from __future__ import absolute_import

import logging
import os

from cachetools.func import lru_cache
from flask import jsonify, render_template, request, url_for
from marblecutter import NoCatalogAvailable, tiling
from marblecutter.catalogs.remote import RemoteCatalog
from marblecutter.formats.optimal import Optimal
from marblecutter.formats.png import PNG
from marblecutter.transformations import Image
from marblecutter.web import app
from mercantile import Tile

from .catalogs import OAMSceneCatalog, OINMetaCatalog

LOG = logging.getLogger(__name__)

IMAGE_TRANSFORMATION = Image()
PNG_FORMAT = PNG()
OPTIMAL_FORMAT = Optimal()

REMOTE_CATALOG_BASE_URL = os.getenv(
    "REMOTE_CATALOG_BASE_URL", "https://api.openaerialmap.org"
)
S3_BUCKET = os.getenv("S3_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "")

# normalize prefix
if S3_PREFIX == "/":
    S3_PREFIX = ""

if not S3_PREFIX.endswith("/"):
    S3_PREFIX += "/"

if S3_PREFIX.startswith("/"):
    S3_PREFIX = S3_PREFIX[1:]


@lru_cache()
def make_catalog(scene_id, scene_idx, image_id=None):
    try:
        if image_id:
            return OINMetaCatalog(
                "s3://{}/{}{}/{}/{}_meta.json".format(
                    S3_BUCKET, S3_PREFIX, scene_id, scene_idx, image_id
                )
            )

        return OAMSceneCatalog(
            "s3://{}{}/{}/{}/scene.json".format(
                S3_BUCKET, S3_PREFIX, scene_id, scene_idx
            )
        )
    except Exception:
        raise NoCatalogAvailable()


@lru_cache()
def make_remote_catalog(type, id):
    try:
        return RemoteCatalog(
            "{}/{}/{}/catalog.json".format(REMOTE_CATALOG_BASE_URL, type, id),
            "{}/{}/{}/{{z}}/{{x}}/{{y}}.json".format(REMOTE_CATALOG_BASE_URL, type, id),
        )
    except Exception:
        raise NoCatalogAvailable()


def make_prefix():
    host = request.headers.get("X-Forwarded-Host", request.headers.get("Host", ""))

    # sniff for API Gateway
    if ".execute-api." in host and ".amazonaws.com" in host:
        return request.headers.get("X-Stage")


@app.route("/<path:id>/<int:scene_idx>/")
@app.route("/<path:id>/<int:scene_idx>/<image_id>/")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/<image_id>/")
def meta(id, scene_idx, image_id=None, prefix=None):
    # prefix is for URL generation only (API Gateway stages); if it matched the
    # URL, it's part of the id
    if prefix is not None:
        id = "/".join([prefix, id])

    catalog = make_catalog(id, scene_idx, image_id)

    meta = {
        "bounds": catalog.bounds,
        "center": catalog.center,
        "maxzoom": catalog.maxzoom,
        "minzoom": catalog.minzoom,
        "name": catalog.name,
        "tilejson": "2.1.0",
    }

    with app.app_context():
        meta["tiles"] = [
            "{}{{z}}/{{x}}/{{y}}".format(
                url_for(
                    "meta",
                    id=id,
                    scene_idx=scene_idx,
                    image_id=image_id,
                    prefix=make_prefix(),
                    _external=True,
                    _scheme="",
                )
            )
        ]

    return jsonify(meta)


@app.route("/user/<path:id>/")
@app.route("/<prefix>/user/<path:id>/")
def user_meta(id, prefix=None):
    catalog = make_remote_catalog("user", id)

    meta = {
        "bounds": catalog.bounds,
        "center": catalog.center,
        "maxzoom": catalog.maxzoom,
        "minzoom": catalog.minzoom,
        "name": catalog.name,
        "tilejson": "2.1.0",
    }

    with app.app_context():
        meta["tiles"] = [
            "{}{{z}}/{{x}}/{{y}}".format(
                url_for(
                    "user_meta", id=id, prefix=make_prefix(), _external=True, _scheme=""
                )
            )
        ]

    return jsonify(meta)


@app.route("/<path:id>/<int:scene_idx>/wmts")
@app.route("/<path:id>/<int:scene_idx>/<image_id>/wmts")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/wmts")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/<image_id>/wmts")
def wmts(id, scene_idx, image_id=None, prefix=None):
    # prefix is for URL generation only (API Gateway stages); if it matched the
    # URL, it's part of the id
    if prefix is not None:
        id = "/".join([prefix, id])

    catalog = make_catalog(id, scene_idx, image_id)

    provider = "OpenAerialMap"
    provider_url = "https://openaerialmap.org/"

    if catalog.provider:
        provider = "{} ({})".format(provider, catalog.provider)

    with app.app_context():
        base_url = url_for(
            "meta",
            id=id,
            scene_idx=scene_idx,
            image_id=image_id,
            prefix=make_prefix(),
            _external=True,
        )

        return render_template(
            "wmts.xml",
            base_url=base_url,
            bounds=catalog.bounds,
            content_type="image/png",
            ext="png",
            id=catalog.id,
            maxzoom=catalog.maxzoom,
            metadata_url=catalog.metadata_url,
            minzoom=catalog.minzoom,
            provider=provider,
            provider_url=provider_url,
            title=catalog.name,
        ), 200, {
            "Content-Type": "application/xml"
        }


@app.route("/user/<path:id>/wmts")
@app.route("/<prefix>/user/<path:id>/wmts")
def user_wmts(id, prefix=None):
    catalog = make_remote_catalog("user", id)

    provider = "OpenAerialMap"
    provider_url = "https://openaerialmap.org/"

    if catalog.provider:
        provider = "{} ({})".format(provider, catalog.provider)

    with app.app_context():
        base_url = url_for("user_meta", id=id, prefix=make_prefix(), _external=True)

        return render_template(
            "wmts.xml",
            base_url=base_url,
            bounds=catalog.bounds,
            content_type="image/png",
            ext="png",
            id=catalog.id,
            maxzoom=catalog.maxzoom,
            metadata_url=catalog.metadata_url,
            minzoom=catalog.minzoom,
            provider=provider,
            provider_url=provider_url,
            title=catalog.name,
        ), 200, {
            "Content-Type": "application/xml"
        }


@app.route("/<path:id>/<int:scene_idx>/preview")
@app.route("/<path:id>/<int:scene_idx>/<image_id>/preview")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/preview")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/<image_id>/preview")
def preview(id, scene_idx, image_id=None, prefix=None):
    # prefix is for URL generation only (API Gateway stages); if it matched the
    # URL, it's part of the id
    if prefix is not None:
        id = "/".join([prefix, id])

    # load the catalog so it will fail if the source doesn't exist
    make_catalog(id, scene_idx, image_id)

    with app.app_context():
        return render_template(
            "preview.html",
            tilejson_url=url_for(
                "meta",
                id=id,
                scene_idx=scene_idx,
                image_id=image_id,
                prefix=make_prefix(),
                _external=True,
                _scheme="",
            ),
        ), 200, {
            "Content-Type": "text/html"
        }


@app.route("/user/<path:id>/preview")
@app.route("/<prefix>/user/<path:id>/preview")
def user_preview(id, prefix=None):
    # load the catalog so it will fail if the source doesn't exist
    make_remote_catalog("user", id)

    with app.app_context():
        return render_template(
            "preview.html",
            tilejson_url=url_for(
                "user_meta", id=id, prefix=make_prefix(), _external=True, _scheme=""
            ),
        ), 200, {
            "Content-Type": "text/html"
        }


@app.route("/<path:id>/<int:scene_idx>/<int:z>/<int:x>/<int:y>.png")
@app.route("/<path:id>/<int:scene_idx>/<int:z>/<int:x>/<int:y>@<int:scale>x.png")
@app.route("/<path:id>/<int:scene_idx>/<image_id>/<int:z>/<int:x>/<int:y>.png")
@app.route(
    "/<path:id>/<int:scene_idx>/<image_id>/<int:z>/<int:x>/<int:y>@<int:scale>x.png"
)
@app.route("/<prefix>/<path:id>/<int:scene_idx>/<image_id>/<int:z>/<int:x>/<int:y>.png")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/<int:z>/<int:x>/<int:y>.png")
@app.route(
    "/<prefix>/<path:id>/<int:scene_idx>/<int:z>/<int:x>/<int:y>@<int:scale>x.png"
)
@app.route(
    "/<prefix>/<path:id>/<int:scene_idx>/<image_id>/<int:z>/<int:x>/<int:y>@<int:scale>x.png"
)
def render_png(id, scene_idx, z, x, y, image_id=None, scale=1, prefix=None):
    # prefix is for URL generation only (API Gateway stages); if it matched the
    # URL, it's part of the id
    if prefix is not None:
        id = "/".join([prefix, id])

    catalog = make_catalog(id, scene_idx, image_id)
    tile = Tile(x, y, z)

    headers, data = tiling.render_tile(
        tile,
        catalog,
        format=PNG_FORMAT,
        transformation=IMAGE_TRANSFORMATION,
        scale=scale,
    )

    headers.update(catalog.headers)

    return data, 200, headers


@app.route("/<path:id>/<int:scene_idx>/<int:z>/<int:x>/<int:y>")
@app.route("/<path:id>/<int:scene_idx>/<int:z>/<int:x>/<int:y>@<int:scale>x")
@app.route("/<path:id>/<int:scene_idx>/<image_id>/<int:z>/<int:x>/<int:y>")
@app.route("/<path:id>/<int:scene_idx>/<image_id>/<int:z>/<int:x>/<int:y>@<int:scale>x")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/<image_id>/<int:z>/<int:x>/<int:y>")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/<int:z>/<int:x>/<int:y>")
@app.route("/<prefix>/<path:id>/<int:scene_idx>/<int:z>/<int:x>/<int:y>@<int:scale>x")
@app.route(
    "/<prefix>/<path:id>/<int:scene_idx>/<image_id>/<int:z>/<int:x>/<int:y>@<int:scale>x"
)
def render(id, scene_idx, z, x, y, image_id=None, scale=1, prefix=None):
    # prefix is for URL generation only (API Gateway stages); if it matched the
    # URL, it's part of the id
    if prefix is not None:
        id = "/".join([prefix, id])

    catalog = make_catalog(id, scene_idx, image_id)
    tile = Tile(x, y, z)

    headers, data = tiling.render_tile(
        tile,
        catalog,
        format=OPTIMAL_FORMAT,
        transformation=IMAGE_TRANSFORMATION,
        scale=scale,
    )

    headers.update(catalog.headers)

    return data, 200, headers


@app.route("/user/<path:id>/<int:z>/<int:x>/<int:y>.png")
@app.route("/user/<path:id>/<int:z>/<int:x>/<int:y>@<int:scale>x.png")
@app.route("/<prefix>/user/<path:id>/<int:z>/<int:x>/<int:y>.png")
@app.route("/<prefix>/user/<path:id>/<int:z>/<int:x>/<int:y>@<int:scale>x.png")
def user_render_png(id, z, x, y, scale=1, prefix=None):
    catalog = make_remote_catalog("user", id)
    tile = Tile(x, y, z)

    headers, data = tiling.render_tile(
        tile,
        catalog,
        format=PNG_FORMAT,
        transformation=IMAGE_TRANSFORMATION,
        scale=scale,
    )

    headers.update(catalog.headers)

    return data, 200, headers


@app.route("/user/<path:id>/<int:z>/<int:x>/<int:y>")
@app.route("/user/<path:id>/<int:z>/<int:x>/<int:y>@<int:scale>x")
@app.route("/<prefix>/user/<path:id>/<int:z>/<int:x>/<int:y>")
@app.route("/<prefix>/user/<path:id>/<int:z>/<int:x>/<int:y>@<int:scale>x")
def user_render(id, z, x, y, scale=1, prefix=None):
    catalog = make_remote_catalog("user", id)
    tile = Tile(x, y, z)

    headers, data = tiling.render_tile(
        tile,
        catalog,
        format=OPTIMAL_FORMAT,
        transformation=IMAGE_TRANSFORMATION,
        scale=scale,
    )

    headers.update(catalog.headers)

    return data, 200, headers
