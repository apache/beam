'use strict';
const MANIFEST = 'flutter-app-manifest';
const TEMP = 'flutter-temp-cache';
const CACHE_NAME = 'flutter-app-cache';
const RESOURCES = {
  "assets/AssetManifest.json": "a2732ef2d96d5c372a973a186d47cbbd",
"assets/assets/png/laptop-dark.png": "aba4f45d43108127e695c0a8b38f726d",
"assets/assets/png/laptop-light.png": "29aaa6f58a3ce649f3dc3a104fd98af5",
"assets/assets/png/profile-website.png": "3622cb9be066227ee962943eacef9650",
"assets/assets/svg/github-logo.svg": "706202edf8b66ab92a234a49332631a8",
"assets/assets/svg/google-logo.svg": "34fa6fbbcc8312858d393322e7768505",
"assets/assets/svg/hint.svg": "4d49c20111179888bacbf98e6cc2a542",
"assets/assets/svg/profile-about.svg": "ef3c8863850d8ae1e2f9bb96e9472c8a",
"assets/assets/svg/profile-delete.svg": "4e1ce30fca2ded1c5c2b85f87df1226b",
"assets/assets/svg/profile-logout.svg": "11a6d6852975f9227dd29496b30a9549",
"assets/assets/svg/solution.svg": "e79f9449fc2bbd28a87e5008d5bcc5bd",
"assets/assets/svg/unit-progress-0.svg": "128144a8734ba2655fae910cfbfeea6b",
"assets/assets/svg/unit-progress-100.svg": "aecdc098aafa2ffe90fa45b1e22a28d1",
"assets/assets/svg/welcome-progress-0.svg": "080c2fc0e4fff0c6dbb824cc30ee4709",
"assets/assets/translations/en.yaml": "2bac80d879bdcbd74e325470f2da1876",
"assets/FontManifest.json": "7b2a36307916a9721811788013e65289",
"assets/fonts/MaterialIcons-Regular.otf": "e7069dfd19b331be16bed984668fe080",
"assets/NOTICES": "51c7a2ef1e02dd9c3efa0f07f57bccc8",
"assets/packages/easy_localization/i18n/ar-DZ.json": "acc0a8eebb2fcee312764600f7cc41ec",
"assets/packages/easy_localization/i18n/ar.json": "acc0a8eebb2fcee312764600f7cc41ec",
"assets/packages/easy_localization/i18n/en-US.json": "5f5fda8715e8bf5116f77f469c5cf493",
"assets/packages/easy_localization/i18n/en.json": "5f5fda8715e8bf5116f77f469c5cf493",
"assets/packages/fluttertoast/assets/toastify.css": "a85675050054f179444bc5ad70ffc635",
"assets/packages/fluttertoast/assets/toastify.js": "e7006a0a033d834ef9414d48db3be6fc",
"assets/packages/playground_components/assets/buttons/reset.svg": "cfe59a2054ac36f12613e3431d0e7d89",
"assets/packages/playground_components/assets/buttons/theme-mode.svg": "4278b3cd7c8fafddd80743456c74d9c1",
"assets/packages/playground_components/assets/notification_icons/error.svg": "2554cfc1fac151435675de56585a3b45",
"assets/packages/playground_components/assets/notification_icons/info.svg": "7dd5326f89fa60863c82947f35f8bd3b",
"assets/packages/playground_components/assets/notification_icons/success.svg": "19312ffea2dbf16b2709a10420f66c7b",
"assets/packages/playground_components/assets/notification_icons/warning.svg": "f27e837f33c2173c33117f52f9e979ad",
"assets/packages/playground_components/assets/png/beam-logo.png": "ac03279e63d47bf1a60e2d666b0e65ff",
"assets/packages/playground_components/assets/svg/drag-horizontal.svg": "9832b1599f27230c363547f35640c9f3",
"assets/packages/playground_components/assets/svg/drag-vertical.svg": "d82f9be135e9e0dba89e232a649800ad",
"assets/packages/playground_components/assets/symbols/go.g.yaml": "32ae1f13538a31f503924166fca28478",
"assets/packages/playground_components/assets/symbols/java.g.yaml": "8e5cac10a11c0121ddbb830d05b906ee",
"assets/packages/playground_components/assets/symbols/python.g.yaml": "e226e2eb78168784204d3e8aafeec2f5",
"assets/packages/playground_components/assets/translations/en.yaml": "958f023b255075772af43ed20bd87d1e",
"canvaskit/canvaskit.js": "97937cb4c2c2073c968525a3e08c86a3",
"canvaskit/canvaskit.wasm": "3de12d898ec208a5f31362cc00f09b9e",
"canvaskit/profiling/canvaskit.js": "c21852696bc1cc82e8894d851c01921a",
"canvaskit/profiling/canvaskit.wasm": "371bc4e204443b0d5e774d64a046eb99",
"favicon.ico": "f6e865ea7d222cdbd2d9238ba90caedd",
"flutter.js": "1cfe996e845b3a8a33f57607e8b09ee4",
"index.html": "ac034208f3263b2c65283cb37cdb72ce",
"/": "ac034208f3263b2c65283cb37cdb72ce",
"main.dart.js": "914624f3221207c3f62de08b11e37e9b",
"manifest.json": "94ea70d82a289739ca1240e4bb71a839",
"version.json": "a9afe384ae9e373fef5433d61d35fddf"
};

// The application shell files that are downloaded before a service worker can
// start.
const CORE = [
  "main.dart.js",
"index.html",
"assets/AssetManifest.json",
"assets/FontManifest.json"];
// During install, the TEMP cache is populated with the application shell files.
self.addEventListener("install", (event) => {
  self.skipWaiting();
  return event.waitUntil(
    caches.open(TEMP).then((cache) => {
      return cache.addAll(
        CORE.map((value) => new Request(value, {'cache': 'reload'})));
    })
  );
});

// During activate, the cache is populated with the temp files downloaded in
// install. If this service worker is upgrading from one with a saved
// MANIFEST, then use this to retain unchanged resource files.
self.addEventListener("activate", function(event) {
  return event.waitUntil(async function() {
    try {
      var contentCache = await caches.open(CACHE_NAME);
      var tempCache = await caches.open(TEMP);
      var manifestCache = await caches.open(MANIFEST);
      var manifest = await manifestCache.match('manifest');
      // When there is no prior manifest, clear the entire cache.
      if (!manifest) {
        await caches.delete(CACHE_NAME);
        contentCache = await caches.open(CACHE_NAME);
        for (var request of await tempCache.keys()) {
          var response = await tempCache.match(request);
          await contentCache.put(request, response);
        }
        await caches.delete(TEMP);
        // Save the manifest to make future upgrades efficient.
        await manifestCache.put('manifest', new Response(JSON.stringify(RESOURCES)));
        return;
      }
      var oldManifest = await manifest.json();
      var origin = self.location.origin;
      for (var request of await contentCache.keys()) {
        var key = request.url.substring(origin.length + 1);
        if (key == "") {
          key = "/";
        }
        // If a resource from the old manifest is not in the new cache, or if
        // the MD5 sum has changed, delete it. Otherwise the resource is left
        // in the cache and can be reused by the new service worker.
        if (!RESOURCES[key] || RESOURCES[key] != oldManifest[key]) {
          await contentCache.delete(request);
        }
      }
      // Populate the cache with the app shell TEMP files, potentially overwriting
      // cache files preserved above.
      for (var request of await tempCache.keys()) {
        var response = await tempCache.match(request);
        await contentCache.put(request, response);
      }
      await caches.delete(TEMP);
      // Save the manifest to make future upgrades efficient.
      await manifestCache.put('manifest', new Response(JSON.stringify(RESOURCES)));
      return;
    } catch (err) {
      // On an unhandled exception the state of the cache cannot be guaranteed.
      console.error('Failed to upgrade service worker: ' + err);
      await caches.delete(CACHE_NAME);
      await caches.delete(TEMP);
      await caches.delete(MANIFEST);
    }
  }());
});

// The fetch handler redirects requests for RESOURCE files to the service
// worker cache.
self.addEventListener("fetch", (event) => {
  if (event.request.method !== 'GET') {
    return;
  }
  var origin = self.location.origin;
  var key = event.request.url.substring(origin.length + 1);
  // Redirect URLs to the index.html
  if (key.indexOf('?v=') != -1) {
    key = key.split('?v=')[0];
  }
  if (event.request.url == origin || event.request.url.startsWith(origin + '/#') || key == '') {
    key = '/';
  }
  // If the URL is not the RESOURCE list then return to signal that the
  // browser should take over.
  if (!RESOURCES[key]) {
    return;
  }
  // If the URL is the index.html, perform an online-first request.
  if (key == '/') {
    return onlineFirst(event);
  }
  event.respondWith(caches.open(CACHE_NAME)
    .then((cache) =>  {
      return cache.match(event.request).then((response) => {
        // Either respond with the cached resource, or perform a fetch and
        // lazily populate the cache only if the resource was successfully fetched.
        return response || fetch(event.request).then((response) => {
          if (response && Boolean(response.ok)) {
            cache.put(event.request, response.clone());
          }
          return response;
        });
      })
    })
  );
});

self.addEventListener('message', (event) => {
  // SkipWaiting can be used to immediately activate a waiting service worker.
  // This will also require a page refresh triggered by the main worker.
  if (event.data === 'skipWaiting') {
    self.skipWaiting();
    return;
  }
  if (event.data === 'downloadOffline') {
    downloadOffline();
    return;
  }
});

// Download offline will check the RESOURCES for all files not in the cache
// and populate them.
async function downloadOffline() {
  var resources = [];
  var contentCache = await caches.open(CACHE_NAME);
  var currentContent = {};
  for (var request of await contentCache.keys()) {
    var key = request.url.substring(origin.length + 1);
    if (key == "") {
      key = "/";
    }
    currentContent[key] = true;
  }
  for (var resourceKey of Object.keys(RESOURCES)) {
    if (!currentContent[resourceKey]) {
      resources.push(resourceKey);
    }
  }
  return contentCache.addAll(resources);
}

// Attempt to download the resource online before falling back to
// the offline cache.
function onlineFirst(event) {
  return event.respondWith(
    fetch(event.request).then((response) => {
      return caches.open(CACHE_NAME).then((cache) => {
        cache.put(event.request, response.clone());
        return response;
      });
    }).catch((error) => {
      return caches.open(CACHE_NAME).then((cache) => {
        return cache.match(event.request).then((response) => {
          if (response != null) {
            return response;
          }
          throw error;
        });
      });
    })
  );
}
