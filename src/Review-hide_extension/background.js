// Copyright 2018 The Chromium Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

'use strict';

$(document).ready(function() {
  $( ".a-profile" ).each(function( idx ) {
    console.log( idx + ": " + this.text() );
  });


// chrome.runtime.onInstalled.addListener(function() {
//   chrome.storage.sync.set({color: '#3aa757'}, function() {
//     console.log("The color is green.");
//   });
//   chrome.declarativeContent.onPageChanged.removeRules(undefined, function() {
//       chrome.declarativeContent.onPageChanged.addRules([{
//         conditions: [new chrome.declarativeContent.PageStateMatcher({
//           pageUrl: {hostEquals: 'www.amazon.com'},
//         })
//         ],
//             actions: [new chrome.declarativeContent.ShowPageAction()]
//       }]);
//   });
// });

// chrome.runtime.onMessage.addListener(
//   function(request, sender, sendResponse) {
//     console.log(sender.tab ?
//                 "from a content script:" + sender.tab.url :
//                 "from the extension");
//     alert(request.greeting);
//     sendResponse({farewell: "goodbye"});
//     // return true;
//   }
// );

    var reviewers = document.getElementsByClassName("a-profile");

    for (var i = 0, max = reviewers.length; i < max; i++) {
         if (reviewers[i.href.includes("AGLHTJEJHQQ763RAURZ2SRP2VKBA")) {
           i.style.backgroundColor = "blue"
         }
    }
});
