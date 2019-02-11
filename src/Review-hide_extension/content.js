'use strict';

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



$(document).ready(function() {

  chrome.runtime.onMessage.addListener(
    function(request, sender, sendResponse) {
      console.log(sender.tab ?
                  "from a content script:" + sender.tab.url :
                  "from the extension");
      // alert(request.selector + ' ' + request.val);
      update(request.selector, request.val);
      sendResponse({farewell: "nope"});
      return true;
    }
  );

  function update(which, val) {
    var reviewers = new Array(); // This will be turned into JSON and sent to the redis interface.
    $( "a.a-profile" ).each(function( idx ) {
      var secondidx = $(this).attr('href').search('/ref'); // second index of the substring containing the user id
      var user_id = $(this).attr('href').substring(26, secondidx); // it always starts at 25
      reviewers.push(user_id);
      if ($(this).attr('href').includes("AGLHTJEJHQQ763RAURZ2SRP2VKBA")) {
        $(this).parent().parent().css("display", "none");
      }
    });
  }
  // var xhr = new XMLHttpRequest();
  // xhr.open("POST", 'https://www.storystreetconsulting.com/wsgi', true);
  // xhr.setRequestHeader('Content-Type', 'application/json');
  // xhr.send(JSON.stringify({
  //   value: reviewers
  // }));
  // console.log(JSON.stringify({
  //   value: reviewers
  // }));
});

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
