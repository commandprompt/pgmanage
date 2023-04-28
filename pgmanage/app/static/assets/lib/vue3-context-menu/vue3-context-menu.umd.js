(function webpackUniversalModuleDefinition(root, factory) {
	if(typeof exports === 'object' && typeof module === 'object')
		module.exports = factory(require("vue"));
	else if(typeof define === 'function' && define.amd)
		define([], factory);
	else if(typeof exports === 'object')
		exports["vue3-context-menu"] = factory(require("vue"));
	else
		root["vue3-context-menu"] = factory(root["Vue"]);
})((typeof self !== 'undefined' ? self : this), (__WEBPACK_EXTERNAL_MODULE__7203__) => {
return /******/ (() => { // webpackBootstrap
/******/ 	var __webpack_modules__ = ({

/***/ 9662:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var isCallable = __webpack_require__(614);
var tryToString = __webpack_require__(6330);

var $TypeError = TypeError;

// `Assert: IsCallable(argument) is true`
module.exports = function (argument) {
  if (isCallable(argument)) return argument;
  throw $TypeError(tryToString(argument) + ' is not a function');
};


/***/ }),

/***/ 6077:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var isCallable = __webpack_require__(614);

var $String = String;
var $TypeError = TypeError;

module.exports = function (argument) {
  if (typeof argument == 'object' || isCallable(argument)) return argument;
  throw $TypeError("Can't set " + $String(argument) + ' as a prototype');
};


/***/ }),

/***/ 9670:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var isObject = __webpack_require__(111);

var $String = String;
var $TypeError = TypeError;

// `Assert: Type(argument) is Object`
module.exports = function (argument) {
  if (isObject(argument)) return argument;
  throw $TypeError($String(argument) + ' is not an object');
};


/***/ }),

/***/ 8533:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var $forEach = (__webpack_require__(2092).forEach);
var arrayMethodIsStrict = __webpack_require__(9341);

var STRICT_METHOD = arrayMethodIsStrict('forEach');

// `Array.prototype.forEach` method implementation
// https://tc39.es/ecma262/#sec-array.prototype.foreach
module.exports = !STRICT_METHOD ? function forEach(callbackfn /* , thisArg */) {
  return $forEach(this, callbackfn, arguments.length > 1 ? arguments[1] : undefined);
// eslint-disable-next-line es-x/no-array-prototype-foreach -- safe
} : [].forEach;


/***/ }),

/***/ 1318:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var toIndexedObject = __webpack_require__(5656);
var toAbsoluteIndex = __webpack_require__(1400);
var lengthOfArrayLike = __webpack_require__(6244);

// `Array.prototype.{ indexOf, includes }` methods implementation
var createMethod = function (IS_INCLUDES) {
  return function ($this, el, fromIndex) {
    var O = toIndexedObject($this);
    var length = lengthOfArrayLike(O);
    var index = toAbsoluteIndex(fromIndex, length);
    var value;
    // Array#includes uses SameValueZero equality algorithm
    // eslint-disable-next-line no-self-compare -- NaN check
    if (IS_INCLUDES && el != el) while (length > index) {
      value = O[index++];
      // eslint-disable-next-line no-self-compare -- NaN check
      if (value != value) return true;
    // Array#indexOf ignores holes, Array#includes - not
    } else for (;length > index; index++) {
      if ((IS_INCLUDES || index in O) && O[index] === el) return IS_INCLUDES || index || 0;
    } return !IS_INCLUDES && -1;
  };
};

module.exports = {
  // `Array.prototype.includes` method
  // https://tc39.es/ecma262/#sec-array.prototype.includes
  includes: createMethod(true),
  // `Array.prototype.indexOf` method
  // https://tc39.es/ecma262/#sec-array.prototype.indexof
  indexOf: createMethod(false)
};


/***/ }),

/***/ 2092:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var bind = __webpack_require__(9974);
var uncurryThis = __webpack_require__(1702);
var IndexedObject = __webpack_require__(8361);
var toObject = __webpack_require__(7908);
var lengthOfArrayLike = __webpack_require__(6244);
var arraySpeciesCreate = __webpack_require__(5417);

var push = uncurryThis([].push);

// `Array.prototype.{ forEach, map, filter, some, every, find, findIndex, filterReject }` methods implementation
var createMethod = function (TYPE) {
  var IS_MAP = TYPE == 1;
  var IS_FILTER = TYPE == 2;
  var IS_SOME = TYPE == 3;
  var IS_EVERY = TYPE == 4;
  var IS_FIND_INDEX = TYPE == 6;
  var IS_FILTER_REJECT = TYPE == 7;
  var NO_HOLES = TYPE == 5 || IS_FIND_INDEX;
  return function ($this, callbackfn, that, specificCreate) {
    var O = toObject($this);
    var self = IndexedObject(O);
    var boundFunction = bind(callbackfn, that);
    var length = lengthOfArrayLike(self);
    var index = 0;
    var create = specificCreate || arraySpeciesCreate;
    var target = IS_MAP ? create($this, length) : IS_FILTER || IS_FILTER_REJECT ? create($this, 0) : undefined;
    var value, result;
    for (;length > index; index++) if (NO_HOLES || index in self) {
      value = self[index];
      result = boundFunction(value, index, O);
      if (TYPE) {
        if (IS_MAP) target[index] = result; // map
        else if (result) switch (TYPE) {
          case 3: return true;              // some
          case 5: return value;             // find
          case 6: return index;             // findIndex
          case 2: push(target, value);      // filter
        } else switch (TYPE) {
          case 4: return false;             // every
          case 7: push(target, value);      // filterReject
        }
      }
    }
    return IS_FIND_INDEX ? -1 : IS_SOME || IS_EVERY ? IS_EVERY : target;
  };
};

module.exports = {
  // `Array.prototype.forEach` method
  // https://tc39.es/ecma262/#sec-array.prototype.foreach
  forEach: createMethod(0),
  // `Array.prototype.map` method
  // https://tc39.es/ecma262/#sec-array.prototype.map
  map: createMethod(1),
  // `Array.prototype.filter` method
  // https://tc39.es/ecma262/#sec-array.prototype.filter
  filter: createMethod(2),
  // `Array.prototype.some` method
  // https://tc39.es/ecma262/#sec-array.prototype.some
  some: createMethod(3),
  // `Array.prototype.every` method
  // https://tc39.es/ecma262/#sec-array.prototype.every
  every: createMethod(4),
  // `Array.prototype.find` method
  // https://tc39.es/ecma262/#sec-array.prototype.find
  find: createMethod(5),
  // `Array.prototype.findIndex` method
  // https://tc39.es/ecma262/#sec-array.prototype.findIndex
  findIndex: createMethod(6),
  // `Array.prototype.filterReject` method
  // https://github.com/tc39/proposal-array-filtering
  filterReject: createMethod(7)
};


/***/ }),

/***/ 1194:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var fails = __webpack_require__(7293);
var wellKnownSymbol = __webpack_require__(5112);
var V8_VERSION = __webpack_require__(7392);

var SPECIES = wellKnownSymbol('species');

module.exports = function (METHOD_NAME) {
  // We can't use this feature detection in V8 since it causes
  // deoptimization and serious performance degradation
  // https://github.com/zloirock/core-js/issues/677
  return V8_VERSION >= 51 || !fails(function () {
    var array = [];
    var constructor = array.constructor = {};
    constructor[SPECIES] = function () {
      return { foo: 1 };
    };
    return array[METHOD_NAME](Boolean).foo !== 1;
  });
};


/***/ }),

/***/ 9341:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var fails = __webpack_require__(7293);

module.exports = function (METHOD_NAME, argument) {
  var method = [][METHOD_NAME];
  return !!method && fails(function () {
    // eslint-disable-next-line no-useless-call -- required for testing
    method.call(null, argument || function () { return 1; }, 1);
  });
};


/***/ }),

/***/ 3658:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var DESCRIPTORS = __webpack_require__(9781);
var isArray = __webpack_require__(3157);

var $TypeError = TypeError;
// eslint-disable-next-line es-x/no-object-getownpropertydescriptor -- safe
var getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;

// Safari < 13 does not throw an error in this case
var SILENT_ON_NON_WRITABLE_LENGTH_SET = DESCRIPTORS && !function () {
  // makes no sense without proper strict mode support
  if (this !== undefined) return true;
  try {
    // eslint-disable-next-line es-x/no-object-defineproperty -- safe
    Object.defineProperty([], 'length', { writable: false }).length = 1;
  } catch (error) {
    return error instanceof TypeError;
  }
}();

module.exports = SILENT_ON_NON_WRITABLE_LENGTH_SET ? function (O, length) {
  if (isArray(O) && !getOwnPropertyDescriptor(O, 'length').writable) {
    throw $TypeError('Cannot set read only .length');
  } return O.length = length;
} : function (O, length) {
  return O.length = length;
};


/***/ }),

/***/ 206:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);

module.exports = uncurryThis([].slice);


/***/ }),

/***/ 7475:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var isArray = __webpack_require__(3157);
var isConstructor = __webpack_require__(4411);
var isObject = __webpack_require__(111);
var wellKnownSymbol = __webpack_require__(5112);

var SPECIES = wellKnownSymbol('species');
var $Array = Array;

// a part of `ArraySpeciesCreate` abstract operation
// https://tc39.es/ecma262/#sec-arrayspeciescreate
module.exports = function (originalArray) {
  var C;
  if (isArray(originalArray)) {
    C = originalArray.constructor;
    // cross-realm fallback
    if (isConstructor(C) && (C === $Array || isArray(C.prototype))) C = undefined;
    else if (isObject(C)) {
      C = C[SPECIES];
      if (C === null) C = undefined;
    }
  } return C === undefined ? $Array : C;
};


/***/ }),

/***/ 5417:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var arraySpeciesConstructor = __webpack_require__(7475);

// `ArraySpeciesCreate` abstract operation
// https://tc39.es/ecma262/#sec-arrayspeciescreate
module.exports = function (originalArray, length) {
  return new (arraySpeciesConstructor(originalArray))(length === 0 ? 0 : length);
};


/***/ }),

/***/ 4326:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);

var toString = uncurryThis({}.toString);
var stringSlice = uncurryThis(''.slice);

module.exports = function (it) {
  return stringSlice(toString(it), 8, -1);
};


/***/ }),

/***/ 648:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var TO_STRING_TAG_SUPPORT = __webpack_require__(1694);
var isCallable = __webpack_require__(614);
var classofRaw = __webpack_require__(4326);
var wellKnownSymbol = __webpack_require__(5112);

var TO_STRING_TAG = wellKnownSymbol('toStringTag');
var $Object = Object;

// ES3 wrong here
var CORRECT_ARGUMENTS = classofRaw(function () { return arguments; }()) == 'Arguments';

// fallback for IE11 Script Access Denied error
var tryGet = function (it, key) {
  try {
    return it[key];
  } catch (error) { /* empty */ }
};

// getting tag from ES6+ `Object.prototype.toString`
module.exports = TO_STRING_TAG_SUPPORT ? classofRaw : function (it) {
  var O, tag, result;
  return it === undefined ? 'Undefined' : it === null ? 'Null'
    // @@toStringTag case
    : typeof (tag = tryGet(O = $Object(it), TO_STRING_TAG)) == 'string' ? tag
    // builtinTag case
    : CORRECT_ARGUMENTS ? classofRaw(O)
    // ES3 arguments fallback
    : (result = classofRaw(O)) == 'Object' && isCallable(O.callee) ? 'Arguments' : result;
};


/***/ }),

/***/ 9920:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var hasOwn = __webpack_require__(2597);
var ownKeys = __webpack_require__(3887);
var getOwnPropertyDescriptorModule = __webpack_require__(1236);
var definePropertyModule = __webpack_require__(3070);

module.exports = function (target, source, exceptions) {
  var keys = ownKeys(source);
  var defineProperty = definePropertyModule.f;
  var getOwnPropertyDescriptor = getOwnPropertyDescriptorModule.f;
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (!hasOwn(target, key) && !(exceptions && hasOwn(exceptions, key))) {
      defineProperty(target, key, getOwnPropertyDescriptor(source, key));
    }
  }
};


/***/ }),

/***/ 8880:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var DESCRIPTORS = __webpack_require__(9781);
var definePropertyModule = __webpack_require__(3070);
var createPropertyDescriptor = __webpack_require__(9114);

module.exports = DESCRIPTORS ? function (object, key, value) {
  return definePropertyModule.f(object, key, createPropertyDescriptor(1, value));
} : function (object, key, value) {
  object[key] = value;
  return object;
};


/***/ }),

/***/ 9114:
/***/ ((module) => {

module.exports = function (bitmap, value) {
  return {
    enumerable: !(bitmap & 1),
    configurable: !(bitmap & 2),
    writable: !(bitmap & 4),
    value: value
  };
};


/***/ }),

/***/ 6135:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var toPropertyKey = __webpack_require__(4948);
var definePropertyModule = __webpack_require__(3070);
var createPropertyDescriptor = __webpack_require__(9114);

module.exports = function (object, key, value) {
  var propertyKey = toPropertyKey(key);
  if (propertyKey in object) definePropertyModule.f(object, propertyKey, createPropertyDescriptor(0, value));
  else object[propertyKey] = value;
};


/***/ }),

/***/ 8052:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var isCallable = __webpack_require__(614);
var definePropertyModule = __webpack_require__(3070);
var makeBuiltIn = __webpack_require__(6339);
var defineGlobalProperty = __webpack_require__(3072);

module.exports = function (O, key, value, options) {
  if (!options) options = {};
  var simple = options.enumerable;
  var name = options.name !== undefined ? options.name : key;
  if (isCallable(value)) makeBuiltIn(value, name, options);
  if (options.global) {
    if (simple) O[key] = value;
    else defineGlobalProperty(key, value);
  } else {
    try {
      if (!options.unsafe) delete O[key];
      else if (O[key]) simple = true;
    } catch (error) { /* empty */ }
    if (simple) O[key] = value;
    else definePropertyModule.f(O, key, {
      value: value,
      enumerable: false,
      configurable: !options.nonConfigurable,
      writable: !options.nonWritable
    });
  } return O;
};


/***/ }),

/***/ 3072:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var global = __webpack_require__(7854);

// eslint-disable-next-line es-x/no-object-defineproperty -- safe
var defineProperty = Object.defineProperty;

module.exports = function (key, value) {
  try {
    defineProperty(global, key, { value: value, configurable: true, writable: true });
  } catch (error) {
    global[key] = value;
  } return value;
};


/***/ }),

/***/ 5117:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var tryToString = __webpack_require__(6330);

var $TypeError = TypeError;

module.exports = function (O, P) {
  if (!delete O[P]) throw $TypeError('Cannot delete property ' + tryToString(P) + ' of ' + tryToString(O));
};


/***/ }),

/***/ 9781:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var fails = __webpack_require__(7293);

// Detect IE8's incomplete defineProperty implementation
module.exports = !fails(function () {
  // eslint-disable-next-line es-x/no-object-defineproperty -- required for testing
  return Object.defineProperty({}, 1, { get: function () { return 7; } })[1] != 7;
});


/***/ }),

/***/ 317:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var global = __webpack_require__(7854);
var isObject = __webpack_require__(111);

var document = global.document;
// typeof document.createElement is 'object' in old IE
var EXISTS = isObject(document) && isObject(document.createElement);

module.exports = function (it) {
  return EXISTS ? document.createElement(it) : {};
};


/***/ }),

/***/ 7207:
/***/ ((module) => {

var $TypeError = TypeError;
var MAX_SAFE_INTEGER = 0x1FFFFFFFFFFFFF; // 2 ** 53 - 1 == 9007199254740991

module.exports = function (it) {
  if (it > MAX_SAFE_INTEGER) throw $TypeError('Maximum allowed index exceeded');
  return it;
};


/***/ }),

/***/ 8113:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var getBuiltIn = __webpack_require__(5005);

module.exports = getBuiltIn('navigator', 'userAgent') || '';


/***/ }),

/***/ 7392:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var global = __webpack_require__(7854);
var userAgent = __webpack_require__(8113);

var process = global.process;
var Deno = global.Deno;
var versions = process && process.versions || Deno && Deno.version;
var v8 = versions && versions.v8;
var match, version;

if (v8) {
  match = v8.split('.');
  // in old Chrome, versions of V8 isn't V8 = Chrome / 10
  // but their correct versions are not interesting for us
  version = match[0] > 0 && match[0] < 4 ? 1 : +(match[0] + match[1]);
}

// BrowserFS NodeJS `process` polyfill incorrectly set `.v8` to `0.0`
// so check `userAgent` even if `.v8` exists, but 0
if (!version && userAgent) {
  match = userAgent.match(/Edge\/(\d+)/);
  if (!match || match[1] >= 74) {
    match = userAgent.match(/Chrome\/(\d+)/);
    if (match) version = +match[1];
  }
}

module.exports = version;


/***/ }),

/***/ 748:
/***/ ((module) => {

// IE8- don't enum bug keys
module.exports = [
  'constructor',
  'hasOwnProperty',
  'isPrototypeOf',
  'propertyIsEnumerable',
  'toLocaleString',
  'toString',
  'valueOf'
];


/***/ }),

/***/ 7762:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var DESCRIPTORS = __webpack_require__(9781);
var fails = __webpack_require__(7293);
var anObject = __webpack_require__(9670);
var create = __webpack_require__(30);
var normalizeStringArgument = __webpack_require__(6277);

var nativeErrorToString = Error.prototype.toString;

var INCORRECT_TO_STRING = fails(function () {
  if (DESCRIPTORS) {
    // Chrome 32- incorrectly call accessor
    // eslint-disable-next-line es-x/no-object-defineproperty -- safe
    var object = create(Object.defineProperty({}, 'name', { get: function () {
      return this === object;
    } }));
    if (nativeErrorToString.call(object) !== 'true') return true;
  }
  // FF10- does not properly handle non-strings
  return nativeErrorToString.call({ message: 1, name: 2 }) !== '2: 1'
    // IE8 does not properly handle defaults
    || nativeErrorToString.call({}) !== 'Error';
});

module.exports = INCORRECT_TO_STRING ? function toString() {
  var O = anObject(this);
  var name = normalizeStringArgument(O.name, 'Error');
  var message = normalizeStringArgument(O.message);
  return !name ? message : !message ? name : name + ': ' + message;
} : nativeErrorToString;


/***/ }),

/***/ 2109:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var global = __webpack_require__(7854);
var getOwnPropertyDescriptor = (__webpack_require__(1236).f);
var createNonEnumerableProperty = __webpack_require__(8880);
var defineBuiltIn = __webpack_require__(8052);
var defineGlobalProperty = __webpack_require__(3072);
var copyConstructorProperties = __webpack_require__(9920);
var isForced = __webpack_require__(4705);

/*
  options.target         - name of the target object
  options.global         - target is the global object
  options.stat           - export as static methods of target
  options.proto          - export as prototype methods of target
  options.real           - real prototype method for the `pure` version
  options.forced         - export even if the native feature is available
  options.bind           - bind methods to the target, required for the `pure` version
  options.wrap           - wrap constructors to preventing global pollution, required for the `pure` version
  options.unsafe         - use the simple assignment of property instead of delete + defineProperty
  options.sham           - add a flag to not completely full polyfills
  options.enumerable     - export as enumerable property
  options.dontCallGetSet - prevent calling a getter on target
  options.name           - the .name of the function if it does not match the key
*/
module.exports = function (options, source) {
  var TARGET = options.target;
  var GLOBAL = options.global;
  var STATIC = options.stat;
  var FORCED, target, key, targetProperty, sourceProperty, descriptor;
  if (GLOBAL) {
    target = global;
  } else if (STATIC) {
    target = global[TARGET] || defineGlobalProperty(TARGET, {});
  } else {
    target = (global[TARGET] || {}).prototype;
  }
  if (target) for (key in source) {
    sourceProperty = source[key];
    if (options.dontCallGetSet) {
      descriptor = getOwnPropertyDescriptor(target, key);
      targetProperty = descriptor && descriptor.value;
    } else targetProperty = target[key];
    FORCED = isForced(GLOBAL ? key : TARGET + (STATIC ? '.' : '#') + key, options.forced);
    // contained in target
    if (!FORCED && targetProperty !== undefined) {
      if (typeof sourceProperty == typeof targetProperty) continue;
      copyConstructorProperties(sourceProperty, targetProperty);
    }
    // add a flag to not completely full polyfills
    if (options.sham || (targetProperty && targetProperty.sham)) {
      createNonEnumerableProperty(sourceProperty, 'sham', true);
    }
    defineBuiltIn(target, key, sourceProperty, options);
  }
};


/***/ }),

/***/ 7293:
/***/ ((module) => {

module.exports = function (exec) {
  try {
    return !!exec();
  } catch (error) {
    return true;
  }
};


/***/ }),

/***/ 2104:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var NATIVE_BIND = __webpack_require__(4374);

var FunctionPrototype = Function.prototype;
var apply = FunctionPrototype.apply;
var call = FunctionPrototype.call;

// eslint-disable-next-line es-x/no-reflect -- safe
module.exports = typeof Reflect == 'object' && Reflect.apply || (NATIVE_BIND ? call.bind(apply) : function () {
  return call.apply(apply, arguments);
});


/***/ }),

/***/ 9974:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);
var aCallable = __webpack_require__(9662);
var NATIVE_BIND = __webpack_require__(4374);

var bind = uncurryThis(uncurryThis.bind);

// optional / simple context binding
module.exports = function (fn, that) {
  aCallable(fn);
  return that === undefined ? fn : NATIVE_BIND ? bind(fn, that) : function (/* ...args */) {
    return fn.apply(that, arguments);
  };
};


/***/ }),

/***/ 4374:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var fails = __webpack_require__(7293);

module.exports = !fails(function () {
  // eslint-disable-next-line es-x/no-function-prototype-bind -- safe
  var test = (function () { /* empty */ }).bind();
  // eslint-disable-next-line no-prototype-builtins -- safe
  return typeof test != 'function' || test.hasOwnProperty('prototype');
});


/***/ }),

/***/ 6916:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var NATIVE_BIND = __webpack_require__(4374);

var call = Function.prototype.call;

module.exports = NATIVE_BIND ? call.bind(call) : function () {
  return call.apply(call, arguments);
};


/***/ }),

/***/ 6530:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var DESCRIPTORS = __webpack_require__(9781);
var hasOwn = __webpack_require__(2597);

var FunctionPrototype = Function.prototype;
// eslint-disable-next-line es-x/no-object-getownpropertydescriptor -- safe
var getDescriptor = DESCRIPTORS && Object.getOwnPropertyDescriptor;

var EXISTS = hasOwn(FunctionPrototype, 'name');
// additional protection from minified / mangled / dropped function names
var PROPER = EXISTS && (function something() { /* empty */ }).name === 'something';
var CONFIGURABLE = EXISTS && (!DESCRIPTORS || (DESCRIPTORS && getDescriptor(FunctionPrototype, 'name').configurable));

module.exports = {
  EXISTS: EXISTS,
  PROPER: PROPER,
  CONFIGURABLE: CONFIGURABLE
};


/***/ }),

/***/ 1702:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var NATIVE_BIND = __webpack_require__(4374);

var FunctionPrototype = Function.prototype;
var bind = FunctionPrototype.bind;
var call = FunctionPrototype.call;
var uncurryThis = NATIVE_BIND && bind.bind(call, call);

module.exports = NATIVE_BIND ? function (fn) {
  return fn && uncurryThis(fn);
} : function (fn) {
  return fn && function () {
    return call.apply(fn, arguments);
  };
};


/***/ }),

/***/ 5005:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var global = __webpack_require__(7854);
var isCallable = __webpack_require__(614);

var aFunction = function (argument) {
  return isCallable(argument) ? argument : undefined;
};

module.exports = function (namespace, method) {
  return arguments.length < 2 ? aFunction(global[namespace]) : global[namespace] && global[namespace][method];
};


/***/ }),

/***/ 8173:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var aCallable = __webpack_require__(9662);
var isNullOrUndefined = __webpack_require__(8554);

// `GetMethod` abstract operation
// https://tc39.es/ecma262/#sec-getmethod
module.exports = function (V, P) {
  var func = V[P];
  return isNullOrUndefined(func) ? undefined : aCallable(func);
};


/***/ }),

/***/ 7854:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var check = function (it) {
  return it && it.Math == Math && it;
};

// https://github.com/zloirock/core-js/issues/86#issuecomment-115759028
module.exports =
  // eslint-disable-next-line es-x/no-global-this -- safe
  check(typeof globalThis == 'object' && globalThis) ||
  check(typeof window == 'object' && window) ||
  // eslint-disable-next-line no-restricted-globals -- safe
  check(typeof self == 'object' && self) ||
  check(typeof __webpack_require__.g == 'object' && __webpack_require__.g) ||
  // eslint-disable-next-line no-new-func -- fallback
  (function () { return this; })() || Function('return this')();


/***/ }),

/***/ 2597:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);
var toObject = __webpack_require__(7908);

var hasOwnProperty = uncurryThis({}.hasOwnProperty);

// `HasOwnProperty` abstract operation
// https://tc39.es/ecma262/#sec-hasownproperty
// eslint-disable-next-line es-x/no-object-hasown -- safe
module.exports = Object.hasOwn || function hasOwn(it, key) {
  return hasOwnProperty(toObject(it), key);
};


/***/ }),

/***/ 3501:
/***/ ((module) => {

module.exports = {};


/***/ }),

/***/ 490:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var getBuiltIn = __webpack_require__(5005);

module.exports = getBuiltIn('document', 'documentElement');


/***/ }),

/***/ 4664:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var DESCRIPTORS = __webpack_require__(9781);
var fails = __webpack_require__(7293);
var createElement = __webpack_require__(317);

// Thanks to IE8 for its funny defineProperty
module.exports = !DESCRIPTORS && !fails(function () {
  // eslint-disable-next-line es-x/no-object-defineproperty -- required for testing
  return Object.defineProperty(createElement('div'), 'a', {
    get: function () { return 7; }
  }).a != 7;
});


/***/ }),

/***/ 8361:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);
var fails = __webpack_require__(7293);
var classof = __webpack_require__(4326);

var $Object = Object;
var split = uncurryThis(''.split);

// fallback for non-array-like ES3 and non-enumerable old V8 strings
module.exports = fails(function () {
  // throws an error in rhino, see https://github.com/mozilla/rhino/issues/346
  // eslint-disable-next-line no-prototype-builtins -- safe
  return !$Object('z').propertyIsEnumerable(0);
}) ? function (it) {
  return classof(it) == 'String' ? split(it, '') : $Object(it);
} : $Object;


/***/ }),

/***/ 9587:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var isCallable = __webpack_require__(614);
var isObject = __webpack_require__(111);
var setPrototypeOf = __webpack_require__(7674);

// makes subclassing work correct for wrapped built-ins
module.exports = function ($this, dummy, Wrapper) {
  var NewTarget, NewTargetPrototype;
  if (
    // it can work only with native `setPrototypeOf`
    setPrototypeOf &&
    // we haven't completely correct pre-ES6 way for getting `new.target`, so use this
    isCallable(NewTarget = dummy.constructor) &&
    NewTarget !== Wrapper &&
    isObject(NewTargetPrototype = NewTarget.prototype) &&
    NewTargetPrototype !== Wrapper.prototype
  ) setPrototypeOf($this, NewTargetPrototype);
  return $this;
};


/***/ }),

/***/ 2788:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);
var isCallable = __webpack_require__(614);
var store = __webpack_require__(5465);

var functionToString = uncurryThis(Function.toString);

// this helper broken in `core-js@3.4.1-3.4.4`, so we can't use `shared` helper
if (!isCallable(store.inspectSource)) {
  store.inspectSource = function (it) {
    return functionToString(it);
  };
}

module.exports = store.inspectSource;


/***/ }),

/***/ 9909:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var NATIVE_WEAK_MAP = __webpack_require__(4811);
var global = __webpack_require__(7854);
var uncurryThis = __webpack_require__(1702);
var isObject = __webpack_require__(111);
var createNonEnumerableProperty = __webpack_require__(8880);
var hasOwn = __webpack_require__(2597);
var shared = __webpack_require__(5465);
var sharedKey = __webpack_require__(6200);
var hiddenKeys = __webpack_require__(3501);

var OBJECT_ALREADY_INITIALIZED = 'Object already initialized';
var TypeError = global.TypeError;
var WeakMap = global.WeakMap;
var set, get, has;

var enforce = function (it) {
  return has(it) ? get(it) : set(it, {});
};

var getterFor = function (TYPE) {
  return function (it) {
    var state;
    if (!isObject(it) || (state = get(it)).type !== TYPE) {
      throw TypeError('Incompatible receiver, ' + TYPE + ' required');
    } return state;
  };
};

if (NATIVE_WEAK_MAP || shared.state) {
  var store = shared.state || (shared.state = new WeakMap());
  var wmget = uncurryThis(store.get);
  var wmhas = uncurryThis(store.has);
  var wmset = uncurryThis(store.set);
  set = function (it, metadata) {
    if (wmhas(store, it)) throw TypeError(OBJECT_ALREADY_INITIALIZED);
    metadata.facade = it;
    wmset(store, it, metadata);
    return metadata;
  };
  get = function (it) {
    return wmget(store, it) || {};
  };
  has = function (it) {
    return wmhas(store, it);
  };
} else {
  var STATE = sharedKey('state');
  hiddenKeys[STATE] = true;
  set = function (it, metadata) {
    if (hasOwn(it, STATE)) throw TypeError(OBJECT_ALREADY_INITIALIZED);
    metadata.facade = it;
    createNonEnumerableProperty(it, STATE, metadata);
    return metadata;
  };
  get = function (it) {
    return hasOwn(it, STATE) ? it[STATE] : {};
  };
  has = function (it) {
    return hasOwn(it, STATE);
  };
}

module.exports = {
  set: set,
  get: get,
  has: has,
  enforce: enforce,
  getterFor: getterFor
};


/***/ }),

/***/ 3157:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var classof = __webpack_require__(4326);

// `IsArray` abstract operation
// https://tc39.es/ecma262/#sec-isarray
// eslint-disable-next-line es-x/no-array-isarray -- safe
module.exports = Array.isArray || function isArray(argument) {
  return classof(argument) == 'Array';
};


/***/ }),

/***/ 614:
/***/ ((module) => {

// `IsCallable` abstract operation
// https://tc39.es/ecma262/#sec-iscallable
module.exports = function (argument) {
  return typeof argument == 'function';
};


/***/ }),

/***/ 4411:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);
var fails = __webpack_require__(7293);
var isCallable = __webpack_require__(614);
var classof = __webpack_require__(648);
var getBuiltIn = __webpack_require__(5005);
var inspectSource = __webpack_require__(2788);

var noop = function () { /* empty */ };
var empty = [];
var construct = getBuiltIn('Reflect', 'construct');
var constructorRegExp = /^\s*(?:class|function)\b/;
var exec = uncurryThis(constructorRegExp.exec);
var INCORRECT_TO_STRING = !constructorRegExp.exec(noop);

var isConstructorModern = function isConstructor(argument) {
  if (!isCallable(argument)) return false;
  try {
    construct(noop, empty, argument);
    return true;
  } catch (error) {
    return false;
  }
};

var isConstructorLegacy = function isConstructor(argument) {
  if (!isCallable(argument)) return false;
  switch (classof(argument)) {
    case 'AsyncFunction':
    case 'GeneratorFunction':
    case 'AsyncGeneratorFunction': return false;
  }
  try {
    // we can't check .prototype since constructors produced by .bind haven't it
    // `Function#toString` throws on some built-it function in some legacy engines
    // (for example, `DOMQuad` and similar in FF41-)
    return INCORRECT_TO_STRING || !!exec(constructorRegExp, inspectSource(argument));
  } catch (error) {
    return true;
  }
};

isConstructorLegacy.sham = true;

// `IsConstructor` abstract operation
// https://tc39.es/ecma262/#sec-isconstructor
module.exports = !construct || fails(function () {
  var called;
  return isConstructorModern(isConstructorModern.call)
    || !isConstructorModern(Object)
    || !isConstructorModern(function () { called = true; })
    || called;
}) ? isConstructorLegacy : isConstructorModern;


/***/ }),

/***/ 4705:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var fails = __webpack_require__(7293);
var isCallable = __webpack_require__(614);

var replacement = /#|\.prototype\./;

var isForced = function (feature, detection) {
  var value = data[normalize(feature)];
  return value == POLYFILL ? true
    : value == NATIVE ? false
    : isCallable(detection) ? fails(detection)
    : !!detection;
};

var normalize = isForced.normalize = function (string) {
  return String(string).replace(replacement, '.').toLowerCase();
};

var data = isForced.data = {};
var NATIVE = isForced.NATIVE = 'N';
var POLYFILL = isForced.POLYFILL = 'P';

module.exports = isForced;


/***/ }),

/***/ 8554:
/***/ ((module) => {

// we can't use just `it == null` since of `document.all` special case
// https://tc39.es/ecma262/#sec-IsHTMLDDA-internal-slot-aec
module.exports = function (it) {
  return it === null || it === undefined;
};


/***/ }),

/***/ 111:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var isCallable = __webpack_require__(614);

var documentAll = typeof document == 'object' && document.all;

// https://tc39.es/ecma262/#sec-IsHTMLDDA-internal-slot
var SPECIAL_DOCUMENT_ALL = typeof documentAll == 'undefined' && documentAll !== undefined;

module.exports = SPECIAL_DOCUMENT_ALL ? function (it) {
  return typeof it == 'object' ? it !== null : isCallable(it) || it === documentAll;
} : function (it) {
  return typeof it == 'object' ? it !== null : isCallable(it);
};


/***/ }),

/***/ 1913:
/***/ ((module) => {

module.exports = false;


/***/ }),

/***/ 2190:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var getBuiltIn = __webpack_require__(5005);
var isCallable = __webpack_require__(614);
var isPrototypeOf = __webpack_require__(7976);
var USE_SYMBOL_AS_UID = __webpack_require__(3307);

var $Object = Object;

module.exports = USE_SYMBOL_AS_UID ? function (it) {
  return typeof it == 'symbol';
} : function (it) {
  var $Symbol = getBuiltIn('Symbol');
  return isCallable($Symbol) && isPrototypeOf($Symbol.prototype, $Object(it));
};


/***/ }),

/***/ 6244:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var toLength = __webpack_require__(7466);

// `LengthOfArrayLike` abstract operation
// https://tc39.es/ecma262/#sec-lengthofarraylike
module.exports = function (obj) {
  return toLength(obj.length);
};


/***/ }),

/***/ 6339:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var fails = __webpack_require__(7293);
var isCallable = __webpack_require__(614);
var hasOwn = __webpack_require__(2597);
var DESCRIPTORS = __webpack_require__(9781);
var CONFIGURABLE_FUNCTION_NAME = (__webpack_require__(6530).CONFIGURABLE);
var inspectSource = __webpack_require__(2788);
var InternalStateModule = __webpack_require__(9909);

var enforceInternalState = InternalStateModule.enforce;
var getInternalState = InternalStateModule.get;
// eslint-disable-next-line es-x/no-object-defineproperty -- safe
var defineProperty = Object.defineProperty;

var CONFIGURABLE_LENGTH = DESCRIPTORS && !fails(function () {
  return defineProperty(function () { /* empty */ }, 'length', { value: 8 }).length !== 8;
});

var TEMPLATE = String(String).split('String');

var makeBuiltIn = module.exports = function (value, name, options) {
  if (String(name).slice(0, 7) === 'Symbol(') {
    name = '[' + String(name).replace(/^Symbol\(([^)]*)\)/, '$1') + ']';
  }
  if (options && options.getter) name = 'get ' + name;
  if (options && options.setter) name = 'set ' + name;
  if (!hasOwn(value, 'name') || (CONFIGURABLE_FUNCTION_NAME && value.name !== name)) {
    if (DESCRIPTORS) defineProperty(value, 'name', { value: name, configurable: true });
    else value.name = name;
  }
  if (CONFIGURABLE_LENGTH && options && hasOwn(options, 'arity') && value.length !== options.arity) {
    defineProperty(value, 'length', { value: options.arity });
  }
  try {
    if (options && hasOwn(options, 'constructor') && options.constructor) {
      if (DESCRIPTORS) defineProperty(value, 'prototype', { writable: false });
    // in V8 ~ Chrome 53, prototypes of some methods, like `Array.prototype.values`, are non-writable
    } else if (value.prototype) value.prototype = undefined;
  } catch (error) { /* empty */ }
  var state = enforceInternalState(value);
  if (!hasOwn(state, 'source')) {
    state.source = TEMPLATE.join(typeof name == 'string' ? name : '');
  } return value;
};

// add fake Function#toString for correct work wrapped methods / constructors with methods like LoDash isNative
// eslint-disable-next-line no-extend-native -- required
Function.prototype.toString = makeBuiltIn(function toString() {
  return isCallable(this) && getInternalState(this).source || inspectSource(this);
}, 'toString');


/***/ }),

/***/ 4758:
/***/ ((module) => {

var ceil = Math.ceil;
var floor = Math.floor;

// `Math.trunc` method
// https://tc39.es/ecma262/#sec-math.trunc
// eslint-disable-next-line es-x/no-math-trunc -- safe
module.exports = Math.trunc || function trunc(x) {
  var n = +x;
  return (n > 0 ? floor : ceil)(n);
};


/***/ }),

/***/ 6277:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var toString = __webpack_require__(1340);

module.exports = function (argument, $default) {
  return argument === undefined ? arguments.length < 2 ? '' : $default : toString(argument);
};


/***/ }),

/***/ 30:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

/* global ActiveXObject -- old IE, WSH */
var anObject = __webpack_require__(9670);
var definePropertiesModule = __webpack_require__(6048);
var enumBugKeys = __webpack_require__(748);
var hiddenKeys = __webpack_require__(3501);
var html = __webpack_require__(490);
var documentCreateElement = __webpack_require__(317);
var sharedKey = __webpack_require__(6200);

var GT = '>';
var LT = '<';
var PROTOTYPE = 'prototype';
var SCRIPT = 'script';
var IE_PROTO = sharedKey('IE_PROTO');

var EmptyConstructor = function () { /* empty */ };

var scriptTag = function (content) {
  return LT + SCRIPT + GT + content + LT + '/' + SCRIPT + GT;
};

// Create object with fake `null` prototype: use ActiveX Object with cleared prototype
var NullProtoObjectViaActiveX = function (activeXDocument) {
  activeXDocument.write(scriptTag(''));
  activeXDocument.close();
  var temp = activeXDocument.parentWindow.Object;
  activeXDocument = null; // avoid memory leak
  return temp;
};

// Create object with fake `null` prototype: use iframe Object with cleared prototype
var NullProtoObjectViaIFrame = function () {
  // Thrash, waste and sodomy: IE GC bug
  var iframe = documentCreateElement('iframe');
  var JS = 'java' + SCRIPT + ':';
  var iframeDocument;
  iframe.style.display = 'none';
  html.appendChild(iframe);
  // https://github.com/zloirock/core-js/issues/475
  iframe.src = String(JS);
  iframeDocument = iframe.contentWindow.document;
  iframeDocument.open();
  iframeDocument.write(scriptTag('document.F=Object'));
  iframeDocument.close();
  return iframeDocument.F;
};

// Check for document.domain and active x support
// No need to use active x approach when document.domain is not set
// see https://github.com/es-shims/es5-shim/issues/150
// variation of https://github.com/kitcambridge/es5-shim/commit/4f738ac066346
// avoid IE GC bug
var activeXDocument;
var NullProtoObject = function () {
  try {
    activeXDocument = new ActiveXObject('htmlfile');
  } catch (error) { /* ignore */ }
  NullProtoObject = typeof document != 'undefined'
    ? document.domain && activeXDocument
      ? NullProtoObjectViaActiveX(activeXDocument) // old IE
      : NullProtoObjectViaIFrame()
    : NullProtoObjectViaActiveX(activeXDocument); // WSH
  var length = enumBugKeys.length;
  while (length--) delete NullProtoObject[PROTOTYPE][enumBugKeys[length]];
  return NullProtoObject();
};

hiddenKeys[IE_PROTO] = true;

// `Object.create` method
// https://tc39.es/ecma262/#sec-object.create
// eslint-disable-next-line es-x/no-object-create -- safe
module.exports = Object.create || function create(O, Properties) {
  var result;
  if (O !== null) {
    EmptyConstructor[PROTOTYPE] = anObject(O);
    result = new EmptyConstructor();
    EmptyConstructor[PROTOTYPE] = null;
    // add "__proto__" for Object.getPrototypeOf polyfill
    result[IE_PROTO] = O;
  } else result = NullProtoObject();
  return Properties === undefined ? result : definePropertiesModule.f(result, Properties);
};


/***/ }),

/***/ 6048:
/***/ ((__unused_webpack_module, exports, __webpack_require__) => {

var DESCRIPTORS = __webpack_require__(9781);
var V8_PROTOTYPE_DEFINE_BUG = __webpack_require__(3353);
var definePropertyModule = __webpack_require__(3070);
var anObject = __webpack_require__(9670);
var toIndexedObject = __webpack_require__(5656);
var objectKeys = __webpack_require__(1956);

// `Object.defineProperties` method
// https://tc39.es/ecma262/#sec-object.defineproperties
// eslint-disable-next-line es-x/no-object-defineproperties -- safe
exports.f = DESCRIPTORS && !V8_PROTOTYPE_DEFINE_BUG ? Object.defineProperties : function defineProperties(O, Properties) {
  anObject(O);
  var props = toIndexedObject(Properties);
  var keys = objectKeys(Properties);
  var length = keys.length;
  var index = 0;
  var key;
  while (length > index) definePropertyModule.f(O, key = keys[index++], props[key]);
  return O;
};


/***/ }),

/***/ 3070:
/***/ ((__unused_webpack_module, exports, __webpack_require__) => {

var DESCRIPTORS = __webpack_require__(9781);
var IE8_DOM_DEFINE = __webpack_require__(4664);
var V8_PROTOTYPE_DEFINE_BUG = __webpack_require__(3353);
var anObject = __webpack_require__(9670);
var toPropertyKey = __webpack_require__(4948);

var $TypeError = TypeError;
// eslint-disable-next-line es-x/no-object-defineproperty -- safe
var $defineProperty = Object.defineProperty;
// eslint-disable-next-line es-x/no-object-getownpropertydescriptor -- safe
var $getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
var ENUMERABLE = 'enumerable';
var CONFIGURABLE = 'configurable';
var WRITABLE = 'writable';

// `Object.defineProperty` method
// https://tc39.es/ecma262/#sec-object.defineproperty
exports.f = DESCRIPTORS ? V8_PROTOTYPE_DEFINE_BUG ? function defineProperty(O, P, Attributes) {
  anObject(O);
  P = toPropertyKey(P);
  anObject(Attributes);
  if (typeof O === 'function' && P === 'prototype' && 'value' in Attributes && WRITABLE in Attributes && !Attributes[WRITABLE]) {
    var current = $getOwnPropertyDescriptor(O, P);
    if (current && current[WRITABLE]) {
      O[P] = Attributes.value;
      Attributes = {
        configurable: CONFIGURABLE in Attributes ? Attributes[CONFIGURABLE] : current[CONFIGURABLE],
        enumerable: ENUMERABLE in Attributes ? Attributes[ENUMERABLE] : current[ENUMERABLE],
        writable: false
      };
    }
  } return $defineProperty(O, P, Attributes);
} : $defineProperty : function defineProperty(O, P, Attributes) {
  anObject(O);
  P = toPropertyKey(P);
  anObject(Attributes);
  if (IE8_DOM_DEFINE) try {
    return $defineProperty(O, P, Attributes);
  } catch (error) { /* empty */ }
  if ('get' in Attributes || 'set' in Attributes) throw $TypeError('Accessors not supported');
  if ('value' in Attributes) O[P] = Attributes.value;
  return O;
};


/***/ }),

/***/ 1236:
/***/ ((__unused_webpack_module, exports, __webpack_require__) => {

var DESCRIPTORS = __webpack_require__(9781);
var call = __webpack_require__(6916);
var propertyIsEnumerableModule = __webpack_require__(5296);
var createPropertyDescriptor = __webpack_require__(9114);
var toIndexedObject = __webpack_require__(5656);
var toPropertyKey = __webpack_require__(4948);
var hasOwn = __webpack_require__(2597);
var IE8_DOM_DEFINE = __webpack_require__(4664);

// eslint-disable-next-line es-x/no-object-getownpropertydescriptor -- safe
var $getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;

// `Object.getOwnPropertyDescriptor` method
// https://tc39.es/ecma262/#sec-object.getownpropertydescriptor
exports.f = DESCRIPTORS ? $getOwnPropertyDescriptor : function getOwnPropertyDescriptor(O, P) {
  O = toIndexedObject(O);
  P = toPropertyKey(P);
  if (IE8_DOM_DEFINE) try {
    return $getOwnPropertyDescriptor(O, P);
  } catch (error) { /* empty */ }
  if (hasOwn(O, P)) return createPropertyDescriptor(!call(propertyIsEnumerableModule.f, O, P), O[P]);
};


/***/ }),

/***/ 8006:
/***/ ((__unused_webpack_module, exports, __webpack_require__) => {

var internalObjectKeys = __webpack_require__(6324);
var enumBugKeys = __webpack_require__(748);

var hiddenKeys = enumBugKeys.concat('length', 'prototype');

// `Object.getOwnPropertyNames` method
// https://tc39.es/ecma262/#sec-object.getownpropertynames
// eslint-disable-next-line es-x/no-object-getownpropertynames -- safe
exports.f = Object.getOwnPropertyNames || function getOwnPropertyNames(O) {
  return internalObjectKeys(O, hiddenKeys);
};


/***/ }),

/***/ 5181:
/***/ ((__unused_webpack_module, exports) => {

// eslint-disable-next-line es-x/no-object-getownpropertysymbols -- safe
exports.f = Object.getOwnPropertySymbols;


/***/ }),

/***/ 7976:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);

module.exports = uncurryThis({}.isPrototypeOf);


/***/ }),

/***/ 6324:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);
var hasOwn = __webpack_require__(2597);
var toIndexedObject = __webpack_require__(5656);
var indexOf = (__webpack_require__(1318).indexOf);
var hiddenKeys = __webpack_require__(3501);

var push = uncurryThis([].push);

module.exports = function (object, names) {
  var O = toIndexedObject(object);
  var i = 0;
  var result = [];
  var key;
  for (key in O) !hasOwn(hiddenKeys, key) && hasOwn(O, key) && push(result, key);
  // Don't enum bug & hidden keys
  while (names.length > i) if (hasOwn(O, key = names[i++])) {
    ~indexOf(result, key) || push(result, key);
  }
  return result;
};


/***/ }),

/***/ 1956:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var internalObjectKeys = __webpack_require__(6324);
var enumBugKeys = __webpack_require__(748);

// `Object.keys` method
// https://tc39.es/ecma262/#sec-object.keys
// eslint-disable-next-line es-x/no-object-keys -- safe
module.exports = Object.keys || function keys(O) {
  return internalObjectKeys(O, enumBugKeys);
};


/***/ }),

/***/ 5296:
/***/ ((__unused_webpack_module, exports) => {

"use strict";

var $propertyIsEnumerable = {}.propertyIsEnumerable;
// eslint-disable-next-line es-x/no-object-getownpropertydescriptor -- safe
var getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;

// Nashorn ~ JDK8 bug
var NASHORN_BUG = getOwnPropertyDescriptor && !$propertyIsEnumerable.call({ 1: 2 }, 1);

// `Object.prototype.propertyIsEnumerable` method implementation
// https://tc39.es/ecma262/#sec-object.prototype.propertyisenumerable
exports.f = NASHORN_BUG ? function propertyIsEnumerable(V) {
  var descriptor = getOwnPropertyDescriptor(this, V);
  return !!descriptor && descriptor.enumerable;
} : $propertyIsEnumerable;


/***/ }),

/***/ 7674:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

/* eslint-disable no-proto -- safe */
var uncurryThis = __webpack_require__(1702);
var anObject = __webpack_require__(9670);
var aPossiblePrototype = __webpack_require__(6077);

// `Object.setPrototypeOf` method
// https://tc39.es/ecma262/#sec-object.setprototypeof
// Works with __proto__ only. Old v8 can't work with null proto objects.
// eslint-disable-next-line es-x/no-object-setprototypeof -- safe
module.exports = Object.setPrototypeOf || ('__proto__' in {} ? function () {
  var CORRECT_SETTER = false;
  var test = {};
  var setter;
  try {
    // eslint-disable-next-line es-x/no-object-getownpropertydescriptor -- safe
    setter = uncurryThis(Object.getOwnPropertyDescriptor(Object.prototype, '__proto__').set);
    setter(test, []);
    CORRECT_SETTER = test instanceof Array;
  } catch (error) { /* empty */ }
  return function setPrototypeOf(O, proto) {
    anObject(O);
    aPossiblePrototype(proto);
    if (CORRECT_SETTER) setter(O, proto);
    else O.__proto__ = proto;
    return O;
  };
}() : undefined);


/***/ }),

/***/ 288:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var TO_STRING_TAG_SUPPORT = __webpack_require__(1694);
var classof = __webpack_require__(648);

// `Object.prototype.toString` method implementation
// https://tc39.es/ecma262/#sec-object.prototype.tostring
module.exports = TO_STRING_TAG_SUPPORT ? {}.toString : function toString() {
  return '[object ' + classof(this) + ']';
};


/***/ }),

/***/ 2140:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var call = __webpack_require__(6916);
var isCallable = __webpack_require__(614);
var isObject = __webpack_require__(111);

var $TypeError = TypeError;

// `OrdinaryToPrimitive` abstract operation
// https://tc39.es/ecma262/#sec-ordinarytoprimitive
module.exports = function (input, pref) {
  var fn, val;
  if (pref === 'string' && isCallable(fn = input.toString) && !isObject(val = call(fn, input))) return val;
  if (isCallable(fn = input.valueOf) && !isObject(val = call(fn, input))) return val;
  if (pref !== 'string' && isCallable(fn = input.toString) && !isObject(val = call(fn, input))) return val;
  throw $TypeError("Can't convert object to primitive value");
};


/***/ }),

/***/ 3887:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var getBuiltIn = __webpack_require__(5005);
var uncurryThis = __webpack_require__(1702);
var getOwnPropertyNamesModule = __webpack_require__(8006);
var getOwnPropertySymbolsModule = __webpack_require__(5181);
var anObject = __webpack_require__(9670);

var concat = uncurryThis([].concat);

// all object keys, includes non-enumerable and symbols
module.exports = getBuiltIn('Reflect', 'ownKeys') || function ownKeys(it) {
  var keys = getOwnPropertyNamesModule.f(anObject(it));
  var getOwnPropertySymbols = getOwnPropertySymbolsModule.f;
  return getOwnPropertySymbols ? concat(keys, getOwnPropertySymbols(it)) : keys;
};


/***/ }),

/***/ 7066:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var anObject = __webpack_require__(9670);

// `RegExp.prototype.flags` getter implementation
// https://tc39.es/ecma262/#sec-get-regexp.prototype.flags
module.exports = function () {
  var that = anObject(this);
  var result = '';
  if (that.hasIndices) result += 'd';
  if (that.global) result += 'g';
  if (that.ignoreCase) result += 'i';
  if (that.multiline) result += 'm';
  if (that.dotAll) result += 's';
  if (that.unicode) result += 'u';
  if (that.unicodeSets) result += 'v';
  if (that.sticky) result += 'y';
  return result;
};


/***/ }),

/***/ 4706:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var call = __webpack_require__(6916);
var hasOwn = __webpack_require__(2597);
var isPrototypeOf = __webpack_require__(7976);
var regExpFlags = __webpack_require__(7066);

var RegExpPrototype = RegExp.prototype;

module.exports = function (R) {
  var flags = R.flags;
  return flags === undefined && !('flags' in RegExpPrototype) && !hasOwn(R, 'flags') && isPrototypeOf(RegExpPrototype, R)
    ? call(regExpFlags, R) : flags;
};


/***/ }),

/***/ 4488:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var isNullOrUndefined = __webpack_require__(8554);

var $TypeError = TypeError;

// `RequireObjectCoercible` abstract operation
// https://tc39.es/ecma262/#sec-requireobjectcoercible
module.exports = function (it) {
  if (isNullOrUndefined(it)) throw $TypeError("Can't call method on " + it);
  return it;
};


/***/ }),

/***/ 7152:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var global = __webpack_require__(7854);
var apply = __webpack_require__(2104);
var isCallable = __webpack_require__(614);
var userAgent = __webpack_require__(8113);
var arraySlice = __webpack_require__(206);
var validateArgumentsLength = __webpack_require__(8053);

var MSIE = /MSIE .\./.test(userAgent); // <- dirty ie9- check
var Function = global.Function;

var wrap = function (scheduler) {
  return MSIE ? function (handler, timeout /* , ...arguments */) {
    var boundArgs = validateArgumentsLength(arguments.length, 1) > 2;
    var fn = isCallable(handler) ? handler : Function(handler);
    var args = boundArgs ? arraySlice(arguments, 2) : undefined;
    return scheduler(boundArgs ? function () {
      apply(fn, this, args);
    } : fn, timeout);
  } : scheduler;
};

// ie9- setTimeout & setInterval additional parameters fix
// https://html.spec.whatwg.org/multipage/timers-and-user-prompts.html#timers
module.exports = {
  // `setTimeout` method
  // https://html.spec.whatwg.org/multipage/timers-and-user-prompts.html#dom-settimeout
  setTimeout: wrap(global.setTimeout),
  // `setInterval` method
  // https://html.spec.whatwg.org/multipage/timers-and-user-prompts.html#dom-setinterval
  setInterval: wrap(global.setInterval)
};


/***/ }),

/***/ 6200:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var shared = __webpack_require__(2309);
var uid = __webpack_require__(9711);

var keys = shared('keys');

module.exports = function (key) {
  return keys[key] || (keys[key] = uid(key));
};


/***/ }),

/***/ 5465:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var global = __webpack_require__(7854);
var defineGlobalProperty = __webpack_require__(3072);

var SHARED = '__core-js_shared__';
var store = global[SHARED] || defineGlobalProperty(SHARED, {});

module.exports = store;


/***/ }),

/***/ 2309:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var IS_PURE = __webpack_require__(1913);
var store = __webpack_require__(5465);

(module.exports = function (key, value) {
  return store[key] || (store[key] = value !== undefined ? value : {});
})('versions', []).push({
  version: '3.25.0',
  mode: IS_PURE ? 'pure' : 'global',
  copyright: '© 2014-2022 Denis Pushkarev (zloirock.ru)',
  license: 'https://github.com/zloirock/core-js/blob/v3.25.0/LICENSE',
  source: 'https://github.com/zloirock/core-js'
});


/***/ }),

/***/ 3111:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);
var requireObjectCoercible = __webpack_require__(4488);
var toString = __webpack_require__(1340);
var whitespaces = __webpack_require__(1361);

var replace = uncurryThis(''.replace);
var whitespace = '[' + whitespaces + ']';
var ltrim = RegExp('^' + whitespace + whitespace + '*');
var rtrim = RegExp(whitespace + whitespace + '*$');

// `String.prototype.{ trim, trimStart, trimEnd, trimLeft, trimRight }` methods implementation
var createMethod = function (TYPE) {
  return function ($this) {
    var string = toString(requireObjectCoercible($this));
    if (TYPE & 1) string = replace(string, ltrim, '');
    if (TYPE & 2) string = replace(string, rtrim, '');
    return string;
  };
};

module.exports = {
  // `String.prototype.{ trimLeft, trimStart }` methods
  // https://tc39.es/ecma262/#sec-string.prototype.trimstart
  start: createMethod(1),
  // `String.prototype.{ trimRight, trimEnd }` methods
  // https://tc39.es/ecma262/#sec-string.prototype.trimend
  end: createMethod(2),
  // `String.prototype.trim` method
  // https://tc39.es/ecma262/#sec-string.prototype.trim
  trim: createMethod(3)
};


/***/ }),

/***/ 6293:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

/* eslint-disable es-x/no-symbol -- required for testing */
var V8_VERSION = __webpack_require__(7392);
var fails = __webpack_require__(7293);

// eslint-disable-next-line es-x/no-object-getownpropertysymbols -- required for testing
module.exports = !!Object.getOwnPropertySymbols && !fails(function () {
  var symbol = Symbol();
  // Chrome 38 Symbol has incorrect toString conversion
  // `get-own-property-symbols` polyfill symbols converted to object are not Symbol instances
  return !String(symbol) || !(Object(symbol) instanceof Symbol) ||
    // Chrome 38-40 symbols are not inherited from DOM collections prototypes to instances
    !Symbol.sham && V8_VERSION && V8_VERSION < 41;
});


/***/ }),

/***/ 863:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);

// `thisNumberValue` abstract operation
// https://tc39.es/ecma262/#sec-thisnumbervalue
module.exports = uncurryThis(1.0.valueOf);


/***/ }),

/***/ 1400:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var toIntegerOrInfinity = __webpack_require__(9303);

var max = Math.max;
var min = Math.min;

// Helper for a popular repeating case of the spec:
// Let integer be ? ToInteger(index).
// If integer < 0, let result be max((length + integer), 0); else let result be min(integer, length).
module.exports = function (index, length) {
  var integer = toIntegerOrInfinity(index);
  return integer < 0 ? max(integer + length, 0) : min(integer, length);
};


/***/ }),

/***/ 5656:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

// toObject with fallback for non-array-like ES3 strings
var IndexedObject = __webpack_require__(8361);
var requireObjectCoercible = __webpack_require__(4488);

module.exports = function (it) {
  return IndexedObject(requireObjectCoercible(it));
};


/***/ }),

/***/ 9303:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var trunc = __webpack_require__(4758);

// `ToIntegerOrInfinity` abstract operation
// https://tc39.es/ecma262/#sec-tointegerorinfinity
module.exports = function (argument) {
  var number = +argument;
  // eslint-disable-next-line no-self-compare -- NaN check
  return number !== number || number === 0 ? 0 : trunc(number);
};


/***/ }),

/***/ 7466:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var toIntegerOrInfinity = __webpack_require__(9303);

var min = Math.min;

// `ToLength` abstract operation
// https://tc39.es/ecma262/#sec-tolength
module.exports = function (argument) {
  return argument > 0 ? min(toIntegerOrInfinity(argument), 0x1FFFFFFFFFFFFF) : 0; // 2 ** 53 - 1 == 9007199254740991
};


/***/ }),

/***/ 7908:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var requireObjectCoercible = __webpack_require__(4488);

var $Object = Object;

// `ToObject` abstract operation
// https://tc39.es/ecma262/#sec-toobject
module.exports = function (argument) {
  return $Object(requireObjectCoercible(argument));
};


/***/ }),

/***/ 7593:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var call = __webpack_require__(6916);
var isObject = __webpack_require__(111);
var isSymbol = __webpack_require__(2190);
var getMethod = __webpack_require__(8173);
var ordinaryToPrimitive = __webpack_require__(2140);
var wellKnownSymbol = __webpack_require__(5112);

var $TypeError = TypeError;
var TO_PRIMITIVE = wellKnownSymbol('toPrimitive');

// `ToPrimitive` abstract operation
// https://tc39.es/ecma262/#sec-toprimitive
module.exports = function (input, pref) {
  if (!isObject(input) || isSymbol(input)) return input;
  var exoticToPrim = getMethod(input, TO_PRIMITIVE);
  var result;
  if (exoticToPrim) {
    if (pref === undefined) pref = 'default';
    result = call(exoticToPrim, input, pref);
    if (!isObject(result) || isSymbol(result)) return result;
    throw $TypeError("Can't convert object to primitive value");
  }
  if (pref === undefined) pref = 'number';
  return ordinaryToPrimitive(input, pref);
};


/***/ }),

/***/ 4948:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var toPrimitive = __webpack_require__(7593);
var isSymbol = __webpack_require__(2190);

// `ToPropertyKey` abstract operation
// https://tc39.es/ecma262/#sec-topropertykey
module.exports = function (argument) {
  var key = toPrimitive(argument, 'string');
  return isSymbol(key) ? key : key + '';
};


/***/ }),

/***/ 1694:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var wellKnownSymbol = __webpack_require__(5112);

var TO_STRING_TAG = wellKnownSymbol('toStringTag');
var test = {};

test[TO_STRING_TAG] = 'z';

module.exports = String(test) === '[object z]';


/***/ }),

/***/ 1340:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var classof = __webpack_require__(648);

var $String = String;

module.exports = function (argument) {
  if (classof(argument) === 'Symbol') throw TypeError('Cannot convert a Symbol value to a string');
  return $String(argument);
};


/***/ }),

/***/ 6330:
/***/ ((module) => {

var $String = String;

module.exports = function (argument) {
  try {
    return $String(argument);
  } catch (error) {
    return 'Object';
  }
};


/***/ }),

/***/ 9711:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var uncurryThis = __webpack_require__(1702);

var id = 0;
var postfix = Math.random();
var toString = uncurryThis(1.0.toString);

module.exports = function (key) {
  return 'Symbol(' + (key === undefined ? '' : key) + ')_' + toString(++id + postfix, 36);
};


/***/ }),

/***/ 3307:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

/* eslint-disable es-x/no-symbol -- required for testing */
var NATIVE_SYMBOL = __webpack_require__(6293);

module.exports = NATIVE_SYMBOL
  && !Symbol.sham
  && typeof Symbol.iterator == 'symbol';


/***/ }),

/***/ 3353:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var DESCRIPTORS = __webpack_require__(9781);
var fails = __webpack_require__(7293);

// V8 ~ Chrome 36-
// https://bugs.chromium.org/p/v8/issues/detail?id=3334
module.exports = DESCRIPTORS && fails(function () {
  // eslint-disable-next-line es-x/no-object-defineproperty -- required for testing
  return Object.defineProperty(function () { /* empty */ }, 'prototype', {
    value: 42,
    writable: false
  }).prototype != 42;
});


/***/ }),

/***/ 8053:
/***/ ((module) => {

var $TypeError = TypeError;

module.exports = function (passed, required) {
  if (passed < required) throw $TypeError('Not enough arguments');
  return passed;
};


/***/ }),

/***/ 4811:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var global = __webpack_require__(7854);
var isCallable = __webpack_require__(614);

var WeakMap = global.WeakMap;

module.exports = isCallable(WeakMap) && /native code/.test(String(WeakMap));


/***/ }),

/***/ 5112:
/***/ ((module, __unused_webpack_exports, __webpack_require__) => {

var global = __webpack_require__(7854);
var shared = __webpack_require__(2309);
var hasOwn = __webpack_require__(2597);
var uid = __webpack_require__(9711);
var NATIVE_SYMBOL = __webpack_require__(6293);
var USE_SYMBOL_AS_UID = __webpack_require__(3307);

var WellKnownSymbolsStore = shared('wks');
var Symbol = global.Symbol;
var symbolFor = Symbol && Symbol['for'];
var createWellKnownSymbol = USE_SYMBOL_AS_UID ? Symbol : Symbol && Symbol.withoutSetter || uid;

module.exports = function (name) {
  if (!hasOwn(WellKnownSymbolsStore, name) || !(NATIVE_SYMBOL || typeof WellKnownSymbolsStore[name] == 'string')) {
    var description = 'Symbol.' + name;
    if (NATIVE_SYMBOL && hasOwn(Symbol, name)) {
      WellKnownSymbolsStore[name] = Symbol[name];
    } else if (USE_SYMBOL_AS_UID && symbolFor) {
      WellKnownSymbolsStore[name] = symbolFor(description);
    } else {
      WellKnownSymbolsStore[name] = createWellKnownSymbol(description);
    }
  } return WellKnownSymbolsStore[name];
};


/***/ }),

/***/ 1361:
/***/ ((module) => {

// a string of all valid unicode whitespaces
module.exports = '\u0009\u000A\u000B\u000C\u000D\u0020\u00A0\u1680\u2000\u2001\u2002' +
  '\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F\u3000\u2028\u2029\uFEFF';


/***/ }),

/***/ 9554:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var $ = __webpack_require__(2109);
var forEach = __webpack_require__(8533);

// `Array.prototype.forEach` method
// https://tc39.es/ecma262/#sec-array.prototype.foreach
// eslint-disable-next-line es-x/no-array-prototype-foreach -- safe
$({ target: 'Array', proto: true, forced: [].forEach != forEach }, {
  forEach: forEach
});


/***/ }),

/***/ 2772:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

/* eslint-disable es-x/no-array-prototype-indexof -- required for testing */
var $ = __webpack_require__(2109);
var uncurryThis = __webpack_require__(1702);
var $indexOf = (__webpack_require__(1318).indexOf);
var arrayMethodIsStrict = __webpack_require__(9341);

var nativeIndexOf = uncurryThis([].indexOf);

var NEGATIVE_ZERO = !!nativeIndexOf && 1 / nativeIndexOf([1], 1, -0) < 0;
var STRICT_METHOD = arrayMethodIsStrict('indexOf');

// `Array.prototype.indexOf` method
// https://tc39.es/ecma262/#sec-array.prototype.indexof
$({ target: 'Array', proto: true, forced: NEGATIVE_ZERO || !STRICT_METHOD }, {
  indexOf: function indexOf(searchElement /* , fromIndex = 0 */) {
    var fromIndex = arguments.length > 1 ? arguments[1] : undefined;
    return NEGATIVE_ZERO
      // convert -0 to +0
      ? nativeIndexOf(this, searchElement, fromIndex) || 0
      : $indexOf(this, searchElement, fromIndex);
  }
});


/***/ }),

/***/ 561:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var $ = __webpack_require__(2109);
var toObject = __webpack_require__(7908);
var toAbsoluteIndex = __webpack_require__(1400);
var toIntegerOrInfinity = __webpack_require__(9303);
var lengthOfArrayLike = __webpack_require__(6244);
var setArrayLength = __webpack_require__(3658);
var doesNotExceedSafeInteger = __webpack_require__(7207);
var arraySpeciesCreate = __webpack_require__(5417);
var createProperty = __webpack_require__(6135);
var deletePropertyOrThrow = __webpack_require__(5117);
var arrayMethodHasSpeciesSupport = __webpack_require__(1194);

var HAS_SPECIES_SUPPORT = arrayMethodHasSpeciesSupport('splice');

var max = Math.max;
var min = Math.min;

// `Array.prototype.splice` method
// https://tc39.es/ecma262/#sec-array.prototype.splice
// with adding support of @@species
$({ target: 'Array', proto: true, forced: !HAS_SPECIES_SUPPORT }, {
  splice: function splice(start, deleteCount /* , ...items */) {
    var O = toObject(this);
    var len = lengthOfArrayLike(O);
    var actualStart = toAbsoluteIndex(start, len);
    var argumentsLength = arguments.length;
    var insertCount, actualDeleteCount, A, k, from, to;
    if (argumentsLength === 0) {
      insertCount = actualDeleteCount = 0;
    } else if (argumentsLength === 1) {
      insertCount = 0;
      actualDeleteCount = len - actualStart;
    } else {
      insertCount = argumentsLength - 2;
      actualDeleteCount = min(max(toIntegerOrInfinity(deleteCount), 0), len - actualStart);
    }
    doesNotExceedSafeInteger(len + insertCount - actualDeleteCount);
    A = arraySpeciesCreate(O, actualDeleteCount);
    for (k = 0; k < actualDeleteCount; k++) {
      from = actualStart + k;
      if (from in O) createProperty(A, k, O[from]);
    }
    A.length = actualDeleteCount;
    if (insertCount < actualDeleteCount) {
      for (k = actualStart; k < len - actualDeleteCount; k++) {
        from = k + actualDeleteCount;
        to = k + insertCount;
        if (from in O) O[to] = O[from];
        else deletePropertyOrThrow(O, to);
      }
      for (k = len; k > len - actualDeleteCount + insertCount; k--) deletePropertyOrThrow(O, k - 1);
    } else if (insertCount > actualDeleteCount) {
      for (k = len - actualDeleteCount; k > actualStart; k--) {
        from = k + actualDeleteCount - 1;
        to = k + insertCount - 1;
        if (from in O) O[to] = O[from];
        else deletePropertyOrThrow(O, to);
      }
    }
    for (k = 0; k < insertCount; k++) {
      O[k + actualStart] = arguments[k + 2];
    }
    setArrayLength(O, len - actualDeleteCount + insertCount);
    return A;
  }
});


/***/ }),

/***/ 3710:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

// TODO: Remove from `core-js@4`
var uncurryThis = __webpack_require__(1702);
var defineBuiltIn = __webpack_require__(8052);

var DatePrototype = Date.prototype;
var INVALID_DATE = 'Invalid Date';
var TO_STRING = 'toString';
var nativeDateToString = uncurryThis(DatePrototype[TO_STRING]);
var thisTimeValue = uncurryThis(DatePrototype.getTime);

// `Date.prototype.toString` method
// https://tc39.es/ecma262/#sec-date.prototype.tostring
if (String(new Date(NaN)) != INVALID_DATE) {
  defineBuiltIn(DatePrototype, TO_STRING, function toString() {
    var value = thisTimeValue(this);
    // eslint-disable-next-line no-self-compare -- NaN check
    return value === value ? nativeDateToString(this) : INVALID_DATE;
  });
}


/***/ }),

/***/ 6647:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

var defineBuiltIn = __webpack_require__(8052);
var errorToString = __webpack_require__(7762);

var ErrorPrototype = Error.prototype;

// `Error.prototype.toString` method fix
// https://tc39.es/ecma262/#sec-error.prototype.tostring
if (ErrorPrototype.toString !== errorToString) {
  defineBuiltIn(ErrorPrototype, 'toString', errorToString);
}


/***/ }),

/***/ 9653:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var DESCRIPTORS = __webpack_require__(9781);
var global = __webpack_require__(7854);
var uncurryThis = __webpack_require__(1702);
var isForced = __webpack_require__(4705);
var defineBuiltIn = __webpack_require__(8052);
var hasOwn = __webpack_require__(2597);
var inheritIfRequired = __webpack_require__(9587);
var isPrototypeOf = __webpack_require__(7976);
var isSymbol = __webpack_require__(2190);
var toPrimitive = __webpack_require__(7593);
var fails = __webpack_require__(7293);
var getOwnPropertyNames = (__webpack_require__(8006).f);
var getOwnPropertyDescriptor = (__webpack_require__(1236).f);
var defineProperty = (__webpack_require__(3070).f);
var thisNumberValue = __webpack_require__(863);
var trim = (__webpack_require__(3111).trim);

var NUMBER = 'Number';
var NativeNumber = global[NUMBER];
var NumberPrototype = NativeNumber.prototype;
var TypeError = global.TypeError;
var arraySlice = uncurryThis(''.slice);
var charCodeAt = uncurryThis(''.charCodeAt);

// `ToNumeric` abstract operation
// https://tc39.es/ecma262/#sec-tonumeric
var toNumeric = function (value) {
  var primValue = toPrimitive(value, 'number');
  return typeof primValue == 'bigint' ? primValue : toNumber(primValue);
};

// `ToNumber` abstract operation
// https://tc39.es/ecma262/#sec-tonumber
var toNumber = function (argument) {
  var it = toPrimitive(argument, 'number');
  var first, third, radix, maxCode, digits, length, index, code;
  if (isSymbol(it)) throw TypeError('Cannot convert a Symbol value to a number');
  if (typeof it == 'string' && it.length > 2) {
    it = trim(it);
    first = charCodeAt(it, 0);
    if (first === 43 || first === 45) {
      third = charCodeAt(it, 2);
      if (third === 88 || third === 120) return NaN; // Number('+0x1') should be NaN, old V8 fix
    } else if (first === 48) {
      switch (charCodeAt(it, 1)) {
        case 66: case 98: radix = 2; maxCode = 49; break; // fast equal of /^0b[01]+$/i
        case 79: case 111: radix = 8; maxCode = 55; break; // fast equal of /^0o[0-7]+$/i
        default: return +it;
      }
      digits = arraySlice(it, 2);
      length = digits.length;
      for (index = 0; index < length; index++) {
        code = charCodeAt(digits, index);
        // parseInt parses a string to a first unavailable symbol
        // but ToNumber should return NaN if a string contains unavailable symbols
        if (code < 48 || code > maxCode) return NaN;
      } return parseInt(digits, radix);
    }
  } return +it;
};

// `Number` constructor
// https://tc39.es/ecma262/#sec-number-constructor
if (isForced(NUMBER, !NativeNumber(' 0o1') || !NativeNumber('0b1') || NativeNumber('+0x1'))) {
  var NumberWrapper = function Number(value) {
    var n = arguments.length < 1 ? 0 : NativeNumber(toNumeric(value));
    var dummy = this;
    // check on 1..constructor(foo) case
    return isPrototypeOf(NumberPrototype, dummy) && fails(function () { thisNumberValue(dummy); })
      ? inheritIfRequired(Object(n), dummy, NumberWrapper) : n;
  };
  for (var keys = DESCRIPTORS ? getOwnPropertyNames(NativeNumber) : (
    // ES3:
    'MAX_VALUE,MIN_VALUE,NaN,NEGATIVE_INFINITY,POSITIVE_INFINITY,' +
    // ES2015 (in case, if modules with ES2015 Number statics required before):
    'EPSILON,MAX_SAFE_INTEGER,MIN_SAFE_INTEGER,isFinite,isInteger,isNaN,isSafeInteger,parseFloat,parseInt,' +
    // ESNext
    'fromString,range'
  ).split(','), j = 0, key; keys.length > j; j++) {
    if (hasOwn(NativeNumber, key = keys[j]) && !hasOwn(NumberWrapper, key)) {
      defineProperty(NumberWrapper, key, getOwnPropertyDescriptor(NativeNumber, key));
    }
  }
  NumberWrapper.prototype = NumberPrototype;
  NumberPrototype.constructor = NumberWrapper;
  defineBuiltIn(global, NUMBER, NumberWrapper, { constructor: true });
}


/***/ }),

/***/ 1539:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

var TO_STRING_TAG_SUPPORT = __webpack_require__(1694);
var defineBuiltIn = __webpack_require__(8052);
var toString = __webpack_require__(288);

// `Object.prototype.toString` method
// https://tc39.es/ecma262/#sec-object.prototype.tostring
if (!TO_STRING_TAG_SUPPORT) {
  defineBuiltIn(Object.prototype, 'toString', toString, { unsafe: true });
}


/***/ }),

/***/ 9714:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

"use strict";

var PROPER_FUNCTION_NAME = (__webpack_require__(6530).PROPER);
var defineBuiltIn = __webpack_require__(8052);
var anObject = __webpack_require__(9670);
var $toString = __webpack_require__(1340);
var fails = __webpack_require__(7293);
var getRegExpFlags = __webpack_require__(4706);

var TO_STRING = 'toString';
var RegExpPrototype = RegExp.prototype;
var nativeToString = RegExpPrototype[TO_STRING];

var NOT_GENERIC = fails(function () { return nativeToString.call({ source: 'a', flags: 'b' }) != '/a/b'; });
// FF44- RegExp#toString has a wrong name
var INCORRECT_NAME = PROPER_FUNCTION_NAME && nativeToString.name != TO_STRING;

// `RegExp.prototype.toString` method
// https://tc39.es/ecma262/#sec-regexp.prototype.tostring
if (NOT_GENERIC || INCORRECT_NAME) {
  defineBuiltIn(RegExp.prototype, TO_STRING, function toString() {
    var R = anObject(this);
    var pattern = $toString(R.source);
    var flags = $toString(getRegExpFlags(R));
    return '/' + pattern + '/' + flags;
  }, { unsafe: true });
}


/***/ }),

/***/ 6815:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

var $ = __webpack_require__(2109);
var global = __webpack_require__(7854);
var setInterval = (__webpack_require__(7152).setInterval);

// ie9- setInterval additional parameters fix
// https://html.spec.whatwg.org/multipage/timers-and-user-prompts.html#dom-setinterval
$({ global: true, bind: true, forced: global.setInterval !== setInterval }, {
  setInterval: setInterval
});


/***/ }),

/***/ 8417:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

var $ = __webpack_require__(2109);
var global = __webpack_require__(7854);
var setTimeout = (__webpack_require__(7152).setTimeout);

// ie9- setTimeout additional parameters fix
// https://html.spec.whatwg.org/multipage/timers-and-user-prompts.html#dom-settimeout
$({ global: true, bind: true, forced: global.setTimeout !== setTimeout }, {
  setTimeout: setTimeout
});


/***/ }),

/***/ 2564:
/***/ ((__unused_webpack_module, __unused_webpack_exports, __webpack_require__) => {

// TODO: Remove this module from `core-js@4` since it's split to modules listed below
__webpack_require__(6815);
__webpack_require__(8417);


/***/ }),

/***/ 3744:
/***/ ((__unused_webpack_module, exports) => {

"use strict";
var __webpack_unused_export__;

__webpack_unused_export__ = ({ value: true });
// runtime helper for setting properties on components
// in a tree-shakable way
exports.Z = (sfc, props) => {
    const target = sfc.__vccOpts || sfc;
    for (const [key, val] of props) {
        target[key] = val;
    }
    return target;
};


/***/ }),

/***/ 7203:
/***/ ((module) => {

"use strict";
module.exports = __WEBPACK_EXTERNAL_MODULE__7203__;

/***/ })

/******/ 	});
/************************************************************************/
/******/ 	// The module cache
/******/ 	var __webpack_module_cache__ = {};
/******/ 	
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/ 		// Check if module is in cache
/******/ 		var cachedModule = __webpack_module_cache__[moduleId];
/******/ 		if (cachedModule !== undefined) {
/******/ 			return cachedModule.exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = __webpack_module_cache__[moduleId] = {
/******/ 			// no module.id needed
/******/ 			// no module.loaded needed
/******/ 			exports: {}
/******/ 		};
/******/ 	
/******/ 		// Execute the module function
/******/ 		__webpack_modules__[moduleId](module, module.exports, __webpack_require__);
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/************************************************************************/
/******/ 	/* webpack/runtime/define property getters */
/******/ 	(() => {
/******/ 		// define getter functions for harmony exports
/******/ 		__webpack_require__.d = (exports, definition) => {
/******/ 			for(var key in definition) {
/******/ 				if(__webpack_require__.o(definition, key) && !__webpack_require__.o(exports, key)) {
/******/ 					Object.defineProperty(exports, key, { enumerable: true, get: definition[key] });
/******/ 				}
/******/ 			}
/******/ 		};
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/global */
/******/ 	(() => {
/******/ 		__webpack_require__.g = (function() {
/******/ 			if (typeof globalThis === 'object') return globalThis;
/******/ 			try {
/******/ 				return this || new Function('return this')();
/******/ 			} catch (e) {
/******/ 				if (typeof window === 'object') return window;
/******/ 			}
/******/ 		})();
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/hasOwnProperty shorthand */
/******/ 	(() => {
/******/ 		__webpack_require__.o = (obj, prop) => (Object.prototype.hasOwnProperty.call(obj, prop))
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/make namespace object */
/******/ 	(() => {
/******/ 		// define __esModule on exports
/******/ 		__webpack_require__.r = (exports) => {
/******/ 			if(typeof Symbol !== 'undefined' && Symbol.toStringTag) {
/******/ 				Object.defineProperty(exports, Symbol.toStringTag, { value: 'Module' });
/******/ 			}
/******/ 			Object.defineProperty(exports, '__esModule', { value: true });
/******/ 		};
/******/ 	})();
/******/ 	
/******/ 	/* webpack/runtime/publicPath */
/******/ 	(() => {
/******/ 		__webpack_require__.p = "";
/******/ 	})();
/******/ 	
/************************************************************************/
var __webpack_exports__ = {};
// This entry need to be wrapped in an IIFE because it need to be in strict mode.
(() => {
"use strict";
// ESM COMPAT FLAG
__webpack_require__.r(__webpack_exports__);

// EXPORTS
__webpack_require__.d(__webpack_exports__, {
  "ContextMenu": () => (/* reexport */ ContextMenu),
  "ContextMenuGroup": () => (/* reexport */ ContextMenuGroup),
  "ContextMenuItem": () => (/* reexport */ ContextMenuItem),
  "ContextMenuSeparator": () => (/* reexport */ ContextMenuSeparator),
  "default": () => (/* binding */ entry_lib)
});

;// CONCATENATED MODULE: ./node_modules/@vue/cli-service/lib/commands/build/setPublicPath.js
/* eslint-disable no-var */
// This file is imported into lib/wc client bundles.

if (typeof window !== 'undefined') {
  var currentScript = window.document.currentScript
  if (false) { var getCurrentScript; }

  var src = currentScript && currentScript.src.match(/(.+\/)[^/]+\.js(\?.*)?$/)
  if (src) {
    __webpack_require__.p = src[1] // eslint-disable-line
  }
}

// Indicate to webpack that this file can be concatenated
/* harmony default export */ const setPublicPath = (null);

// EXTERNAL MODULE: external {"commonjs":"vue","commonjs2":"vue","root":"Vue"}
var external_commonjs_vue_commonjs2_vue_root_Vue_ = __webpack_require__(7203);
;// CONCATENATED MODULE: ./src/ContextMenuMutex.ts
var currentOpenedContextMenu = null;
function addOpenedContextMenu(inst) {
  if (currentOpenedContextMenu) closeContextMenu();
  currentOpenedContextMenu = inst;
}
function removeOpenedContextMenu(inst) {
  if (inst === currentOpenedContextMenu) currentOpenedContextMenu = null;
}
/**
 * Close the currently open menu
 */

function closeContextMenu() {
  if (currentOpenedContextMenu) {
    currentOpenedContextMenu.closeMenu();
    currentOpenedContextMenu = null;
  }
}
// EXTERNAL MODULE: ./node_modules/core-js/modules/es.error.to-string.js
var es_error_to_string = __webpack_require__(6647);
// EXTERNAL MODULE: ./node_modules/core-js/modules/es.date.to-string.js
var es_date_to_string = __webpack_require__(3710);
// EXTERNAL MODULE: ./node_modules/core-js/modules/es.object.to-string.js
var es_object_to_string = __webpack_require__(1539);
// EXTERNAL MODULE: ./node_modules/core-js/modules/es.regexp.to-string.js
var es_regexp_to_string = __webpack_require__(9714);
;// CONCATENATED MODULE: ./src/ContextMenuDefine.ts
/**
 * Default config
 */
var MenuConstOptions = {
  defaultMinWidth: 100,
  defaultMaxWidth: 600,
  defaultZindex: 100,
  defaultAdjustPadding: 10
};
;// CONCATENATED MODULE: ./src/ContextMenuUtils.ts






function getTop(e, stopNode) {
  var offset = e.offsetTop;

  if (e.offsetParent != null && e.offsetParent != stopNode) {
    offset -= e.offsetParent.scrollTop;
    offset += getTop(e.offsetParent, stopNode);
  }

  return offset;
}
function getLeft(e, stopNode) {
  var offset = e.offsetLeft;

  if (e.offsetParent != null && e.offsetParent != stopNode) {
    offset -= e.offsetParent.scrollLeft;
    offset += getLeft(e.offsetParent, stopNode);
  }

  return offset;
}
/**
 * If your `body` element is in a scaled state (e.g. `transform: scale(0.5)`),
 * this may lead to the wrong position of the menu display.
 * You can use this function to transform the menu display position:
 *
 * ```ts
 *
  import ContextMenu from '@imengyu/vue3-context-menu'

  function onContextMenu(e: MouseEvent) {
    const scaledPosition = ContextMenu.transformMenuPosition(e.target as HTMLElement, e.offsetX, e.offsetY);
    //Full code of menuData is in `/examples/views/InScaledBody.vue`
    menuData.x = scaledPosition.x;
    menuData.y = scaledPosition.y;
    //show menu
    ContextMenu.showContextMenu(menuData);
  }
  * ```
  * @param e Current click element
  * @param offsetX MouseEvent.offsetX
  * @param offsetY MouseEvent.offsetY
  */

function transformMenuPosition(e, offsetX, offsetY, container) {
  return {
    x: getLeft(e, container) + offsetX,
    y: getTop(e, container) + offsetY
  };
}
function removeContainer(container) {
  var _container$parentNode;

  (_container$parentNode = container.parentNode) === null || _container$parentNode === void 0 ? void 0 : _container$parentNode.removeChild(container);
}
var DEFAULT_CONTAINER_ID = 'mx-menu-default-container';
function genContainer(options) {
  var getContainer = options.getContainer,
      zIndex = options.zIndex;

  if (getContainer) {
    var _container = typeof getContainer === 'function' ? getContainer() : getContainer;

    if (_container) {
      _container.style.zIndex = (zIndex === null || zIndex === void 0 ? void 0 : zIndex.toString()) || MenuConstOptions.defaultZindex.toString();
      return {
        container: _container,
        isNew: false
      };
    }
  }

  var container = document.getElementById(DEFAULT_CONTAINER_ID);
  if (container) document.body.removeChild(container);
  container = document.createElement('div');
  container.setAttribute('id', DEFAULT_CONTAINER_ID);
  container.setAttribute('class', 'mx-menu-ghost-host fullscreen');
  container.style.zIndex = (zIndex === null || zIndex === void 0 ? void 0 : zIndex.toString()) || MenuConstOptions.defaultZindex.toString(); //container.style.height = document.body.scrollHeight.toString() + "px"
  //container.style.width = document.body.scrollWidth.toString() + "px"

  document.body.appendChild(container);
  return {
    container: container,
    isNew: true
  };
}
/**
 * Render a VNode
 */

var VNodeRender = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.defineComponent)({
  props: {
    vnode: {
      type: [Object, Function],
      "default": null
    },
    data: {
      type: Object,
      "default": null
    }
  },
  setup: function setup(props) {
    var _toRefs = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.toRefs)(props),
        vnode = _toRefs.vnode,
        data = _toRefs.data;

    return function () {
      return typeof vnode.value === 'function' ? vnode.value(data.value) : vnode.value;
    };
  }
});
;// CONCATENATED MODULE: ./node_modules/@babel/runtime/helpers/esm/defineProperty.js
function _defineProperty(obj, key, value) {
  if (key in obj) {
    Object.defineProperty(obj, key, {
      value: value,
      enumerable: true,
      configurable: true,
      writable: true
    });
  } else {
    obj[key] = value;
  }

  return obj;
}
;// CONCATENATED MODULE: ./node_modules/@babel/runtime/helpers/esm/objectSpread2.js


function ownKeys(object, enumerableOnly) {
  var keys = Object.keys(object);

  if (Object.getOwnPropertySymbols) {
    var symbols = Object.getOwnPropertySymbols(object);
    enumerableOnly && (symbols = symbols.filter(function (sym) {
      return Object.getOwnPropertyDescriptor(object, sym).enumerable;
    })), keys.push.apply(keys, symbols);
  }

  return keys;
}

function _objectSpread2(target) {
  for (var i = 1; i < arguments.length; i++) {
    var source = null != arguments[i] ? arguments[i] : {};
    i % 2 ? ownKeys(Object(source), !0).forEach(function (key) {
      _defineProperty(target, key, source[key]);
    }) : Object.getOwnPropertyDescriptors ? Object.defineProperties(target, Object.getOwnPropertyDescriptors(source)) : ownKeys(Object(source)).forEach(function (key) {
      Object.defineProperty(target, key, Object.getOwnPropertyDescriptor(source, key));
    });
  }

  return target;
}
// EXTERNAL MODULE: ./node_modules/core-js/modules/web.timers.js
var web_timers = __webpack_require__(2564);
;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js!./node_modules/ts-loader/index.js??clonedRuleSet-86.use[2]!./node_modules/vue-loader/dist/templateLoader.js??ruleSet[1].rules[4]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextSubMenu.vue?vue&type=template&id=60fc14ea&ts=true

var _hoisted_1 = {
  "class": "mx-context-menu-scroll",
  ref: "scroll"
};
function render(_ctx, _cache, $props, $setup, $data, $options) {
  var _component_ContextSubMenu = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.resolveComponent)("ContextSubMenu", true);

  var _component_ContextMenuItem = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.resolveComponent)("ContextMenuItem");

  var _component_VNodeRender = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.resolveComponent)("VNodeRender");

  var _component_ContextMenuSeparator = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.resolveComponent)("ContextMenuSeparator");

  var _component_ContextMenuIconRight = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.resolveComponent)("ContextMenuIconRight");

  return (0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)("div", {
    "class": (0,external_commonjs_vue_commonjs2_vue_root_Vue_.normalizeClass)('mx-context-menu ' + (_ctx.options.customClass ? _ctx.options.customClass : '') + ' ' + _ctx.globalTheme),
    style: (0,external_commonjs_vue_commonjs2_vue_root_Vue_.normalizeStyle)({
      maxWidth: _ctx.maxWidth ? _ctx.maxWidth : "".concat(_ctx.constOptions.defaultMaxWidth, "px"),
      minWidth: _ctx.minWidth ? _ctx.minWidth : "".concat(_ctx.constOptions.defaultMinWidth, "px"),
      maxHeight: _ctx.overflow && _ctx.maxHeight > 0 ? "".concat(_ctx.maxHeight, "px") : undefined,
      zIndex: _ctx.zIndex,
      left: "".concat(_ctx.position.x, "px"),
      top: "".concat(_ctx.position.y, "px")
    }),
    "data-type": "ContextSubMenu",
    onClick: _cache[2] || (_cache[2] = //@ts-ignore
    function () {
      return _ctx.onSubMenuBodyClick && _ctx.onSubMenuBodyClick.apply(_ctx, arguments);
    })
  }, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("div", {
    "class": "mx-context-menu-items",
    ref: "menu",
    style: (0,external_commonjs_vue_commonjs2_vue_root_Vue_.normalizeStyle)({
      top: "".concat(_ctx.scrollValue, "px")
    })
  }, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderSlot)(_ctx.$slots, "default", {}, function () {
    return [((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(true), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)(external_commonjs_vue_commonjs2_vue_root_Vue_.Fragment, null, (0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderList)(_ctx.items, function (item, i) {
      return (0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)(external_commonjs_vue_commonjs2_vue_root_Vue_.Fragment, {
        key: i
      }, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createVNode)(_component_ContextMenuItem, {
        clickHandler: item.onClick,
        disabled: item.disabled,
        hidden: item.hidden,
        icon: item.icon,
        iconFontClass: item.iconFontClass,
        svgIcon: item.svgIcon,
        svgProps: item.svgProps,
        label: item.label,
        customRender: item.customRender,
        customClass: item.customClass,
        checked: item.checked,
        shortcut: item.shortcut,
        clickClose: item.clickClose,
        clickableWhenHasChildren: item.clickableWhenHasChildren,
        preserveIconWidth: item.preserveIconWidth !== undefined ? item.preserveIconWidth : _ctx.options.preserveIconWidth,
        showRightArrow: item.children && item.children.length > 0,
        hasChildren: item.children && item.children.length > 0
      }, (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createSlots)({
        _: 2
      }, [item.children && item.children.length > 0 ? {
        name: "submenu",
        fn: (0,external_commonjs_vue_commonjs2_vue_root_Vue_.withCtx)(function () {
          return [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createVNode)(_component_ContextSubMenu, {
            items: item.children,
            maxWidth: item.maxWidth,
            minWidth: item.minWidth,
            adjustPosition: item.adjustSubMenuPosition !== undefined ? item.adjustSubMenuPosition : _ctx.options.adjustPosition
          }, null, 8, ["items", "maxWidth", "minWidth", "adjustPosition"])];
        }),
        key: "0"
      } : undefined]), 1032, ["clickHandler", "disabled", "hidden", "icon", "iconFontClass", "svgIcon", "svgProps", "label", "customRender", "customClass", "checked", "shortcut", "clickClose", "clickableWhenHasChildren", "preserveIconWidth", "showRightArrow", "hasChildren"]), item.hidden !== true && item.divided && _ctx.globalHasSlot('separatorRender') ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
        key: 0,
        vnode: function vnode() {
          return _ctx.globalRenderSlot('separatorRender', {});
        }
      }, null, 8, ["vnode"])) : item.hidden !== true && item.divided ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_ContextMenuSeparator, {
        key: 1
      })) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true)], 64);
    }), 128))];
  })], 4), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("div", _hoisted_1, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.withDirectives)((0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("div", {
    "class": "mx-context-menu-updown mx-context-no-clickable up",
    onClick: _cache[0] || (_cache[0] = function ($event) {
      return _ctx.onScroll(false);
    })
  }, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createVNode)(_component_ContextMenuIconRight)], 512), [[external_commonjs_vue_commonjs2_vue_root_Vue_.vShow, _ctx.overflow && _ctx.scrollValue < 0]]), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.withDirectives)((0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("div", {
    "class": "mx-context-menu-updown mx-context-no-clickable down",
    onClick: _cache[1] || (_cache[1] = function ($event) {
      return _ctx.onScroll(true);
    })
  }, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createVNode)(_component_ContextMenuIconRight)], 512), [[external_commonjs_vue_commonjs2_vue_root_Vue_.vShow, _ctx.overflow && _ctx.scrollValue > -_ctx.scrollHeight]])], 512)], 6);
}
;// CONCATENATED MODULE: ./src/ContextSubMenu.vue?vue&type=template&id=60fc14ea&ts=true

// EXTERNAL MODULE: ./node_modules/core-js/modules/es.number.constructor.js
var es_number_constructor = __webpack_require__(9653);
// EXTERNAL MODULE: ./node_modules/core-js/modules/es.array.index-of.js
var es_array_index_of = __webpack_require__(2772);
// EXTERNAL MODULE: ./node_modules/core-js/modules/es.array.for-each.js
var es_array_for_each = __webpack_require__(9554);
// EXTERNAL MODULE: ./node_modules/core-js/modules/es.array.splice.js
var es_array_splice = __webpack_require__(561);
;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js!./node_modules/ts-loader/index.js??clonedRuleSet-86.use[2]!./node_modules/vue-loader/dist/templateLoader.js??ruleSet[1].rules[4]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextMenuItem.vue?vue&type=template&id=027b05a5&ts=true

var ContextMenuItemvue_type_template_id_027b05a5_ts_true_hoisted_1 = {
  key: 0,
  "class": "mx-context-menu-item-wrapper",
  ref: "menuItemRef",
  "data-type": "ContextMenuItem"
};
var _hoisted_2 = {
  "class": "mx-item-row"
};
var _hoisted_3 = ["xlink:href"];
var _hoisted_4 = {
  key: 1,
  "class": "label"
};
var _hoisted_5 = {
  "class": "mx-item-row"
};
var _hoisted_6 = {
  "class": "mx-shortcut"
};
function ContextMenuItemvue_type_template_id_027b05a5_ts_true_render(_ctx, _cache, $props, $setup, $data, $options) {
  var _component_VNodeRender = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.resolveComponent)("VNodeRender");

  var _component_ContextMenuIconCheck = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.resolveComponent)("ContextMenuIconCheck");

  var _component_ContextMenuIconRight = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.resolveComponent)("ContextMenuIconRight");

  return !_ctx.hidden ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)("div", ContextMenuItemvue_type_template_id_027b05a5_ts_true_hoisted_1, [_ctx.globalHasSlot('itemRender') ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
    key: 0,
    vnode: function vnode() {
      return _ctx.globalRenderSlot('itemRender', _ctx.getItemDataForChildren());
    }
  }, null, 8, ["vnode"])) : _ctx.customRender ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
    key: 1,
    vnode: _ctx.customRender,
    data: _ctx.getItemDataForChildren()
  }, null, 8, ["vnode", "data"])) : ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)("div", {
    key: 2,
    "class": (0,external_commonjs_vue_commonjs2_vue_root_Vue_.normalizeClass)(['mx-context-menu-item', _ctx.disabled ? 'disabled' : '', _ctx.keyBoardFocusMenu ? 'keyboard-focus' : '', _ctx.customClass ? ' ' + _ctx.customClass : '', _ctx.showSubMenu ? 'open' : '']),
    onClick: _cache[0] || (_cache[0] = //@ts-ignore
    function () {
      return _ctx.onClick && _ctx.onClick.apply(_ctx, arguments);
    }),
    onMouseenter: _cache[1] || (_cache[1] = //@ts-ignore
    function () {
      return _ctx.onMouseEnter && _ctx.onMouseEnter.apply(_ctx, arguments);
    })
  }, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderSlot)(_ctx.$slots, "default", {}, function () {
    return [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("div", _hoisted_2, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("div", {
      "class": (0,external_commonjs_vue_commonjs2_vue_root_Vue_.normalizeClass)(['mx-icon-placeholder', _ctx.preserveIconWidth ? 'preserve-width' : ''])
    }, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderSlot)(_ctx.$slots, "icon", {}, function () {
      return [_ctx.globalHasSlot('itemIconRender') ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
        key: 0,
        vnode: function vnode() {
          return _ctx.globalRenderSlot('itemIconRender', _ctx.getItemDataForChildren());
        }
      }, null, 8, ["vnode"])) : typeof _ctx.svgIcon === 'string' && _ctx.svgIcon ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)("svg", (0,external_commonjs_vue_commonjs2_vue_root_Vue_.mergeProps)({
        key: 1,
        "class": "icon svg"
      }, _ctx.svgProps), [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("use", {
        "xlink:href": _ctx.svgIcon
      }, null, 8, _hoisted_3)], 16)) : typeof _ctx.icon !== 'string' ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
        key: 2,
        vnode: _ctx.icon,
        data: _ctx.icon
      }, null, 8, ["vnode", "data"])) : typeof _ctx.icon === 'string' && _ctx.icon !== '' ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)("i", {
        key: 3,
        "class": (0,external_commonjs_vue_commonjs2_vue_root_Vue_.normalizeClass)(_ctx.icon + ' icon ' + _ctx.iconFontClass + ' ' + _ctx.globalIconFontClass)
      }, null, 2)) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true)];
    }), _ctx.checked ? (0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderSlot)(_ctx.$slots, "check", {
      key: 0
    }, function () {
      return [_ctx.globalHasSlot('itemCheckRender') ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
        key: 0,
        vnode: function vnode() {
          return _ctx.globalRenderSlot('itemCheckRender', _ctx.getItemDataForChildren());
        }
      }, null, 8, ["vnode"])) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createVNode)(_component_ContextMenuIconCheck)];
    }) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true)], 2), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderSlot)(_ctx.$slots, "label", {}, function () {
      return [_ctx.globalHasSlot('itemLabelRender') ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
        key: 0,
        vnode: function vnode() {
          return _ctx.globalRenderSlot('itemLabelRender', _ctx.getItemDataForChildren());
        }
      }, null, 8, ["vnode"])) : typeof _ctx.label === 'string' ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)("span", _hoisted_4, (0,external_commonjs_vue_commonjs2_vue_root_Vue_.toDisplayString)(_ctx.label), 1)) : ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
        key: 2,
        vnode: _ctx.label,
        data: _ctx.label
      }, null, 8, ["vnode", "data"]))];
    })]), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("div", _hoisted_5, [_ctx.shortcut ? (0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderSlot)(_ctx.$slots, "shortcut", {
      key: 0
    }, function () {
      return [_ctx.globalHasSlot('itemShortcutRender') ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
        key: 0,
        vnode: function vnode() {
          return _ctx.globalRenderSlot('itemShortcutRender', _ctx.getItemDataForChildren());
        }
      }, null, 8, ["vnode"])) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("span", _hoisted_6, (0,external_commonjs_vue_commonjs2_vue_root_Vue_.toDisplayString)(_ctx.shortcut), 1)];
    }) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true), _ctx.showRightArrow ? (0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderSlot)(_ctx.$slots, "rightArrow", {
      key: 1
    }, function () {
      return [_ctx.globalHasSlot('itemRightArrowRender') ? ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createBlock)(_component_VNodeRender, {
        key: 0,
        vnode: function vnode() {
          return _ctx.globalRenderSlot('itemRightArrowRender', _ctx.getItemDataForChildren());
        }
      }, null, 8, ["vnode"])) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createVNode)(_component_ContextMenuIconRight)];
    }) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true)])];
  })], 34)), _ctx.showSubMenu ? (0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderSlot)(_ctx.$slots, "submenu", {
    key: 3
  }) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true)], 512)) : (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createCommentVNode)("", true);
}
;// CONCATENATED MODULE: ./src/ContextMenuItem.vue?vue&type=template&id=027b05a5&ts=true

;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js??clonedRuleSet-85.use[1]!./node_modules/babel-loader/lib/index.js!./node_modules/vue-loader/dist/templateLoader.js??ruleSet[1].rules[4]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextMenuIconCheck.vue?vue&type=template&id=f3b78eb6

var ContextMenuIconCheckvue_type_template_id_f3b78eb6_hoisted_1 = {
  "class": "mx-checked-mark",
  "aria-hidden": "true",
  viewBox: "0 0 1024 1024"
};

var ContextMenuIconCheckvue_type_template_id_f3b78eb6_hoisted_2 = /*#__PURE__*/(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("path", {
  d: "M129.3,428.6L52,512l345,372.5l575-620.8l-69.5-75L400.4,718.2L129.3,428.6z"
}, null, -1);

var ContextMenuIconCheckvue_type_template_id_f3b78eb6_hoisted_3 = [ContextMenuIconCheckvue_type_template_id_f3b78eb6_hoisted_2];
function ContextMenuIconCheckvue_type_template_id_f3b78eb6_render(_ctx, _cache) {
  return (0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)("svg", ContextMenuIconCheckvue_type_template_id_f3b78eb6_hoisted_1, ContextMenuIconCheckvue_type_template_id_f3b78eb6_hoisted_3);
}
;// CONCATENATED MODULE: ./src/ContextMenuIconCheck.vue?vue&type=template&id=f3b78eb6

// EXTERNAL MODULE: ./node_modules/vue-loader/dist/exportHelper.js
var exportHelper = __webpack_require__(3744);
;// CONCATENATED MODULE: ./src/ContextMenuIconCheck.vue

const script = {}

;
const __exports__ = /*#__PURE__*/(0,exportHelper/* default */.Z)(script, [['render',ContextMenuIconCheckvue_type_template_id_f3b78eb6_render]])

/* harmony default export */ const ContextMenuIconCheck = (__exports__);
;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js??clonedRuleSet-85.use[1]!./node_modules/babel-loader/lib/index.js!./node_modules/vue-loader/dist/templateLoader.js??ruleSet[1].rules[4]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextMenuIconRight.vue?vue&type=template&id=41a351a6

var ContextMenuIconRightvue_type_template_id_41a351a6_hoisted_1 = {
  "class": "mx-right-arrow",
  "aria-hidden": "true",
  viewBox: "0 0 1024 1024"
};

var ContextMenuIconRightvue_type_template_id_41a351a6_hoisted_2 = /*#__PURE__*/(0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementVNode)("path", {
  d: "M307.018 49.445c11.517 0 23.032 4.394 31.819 13.18L756.404 480.18c8.439 8.438 13.181 19.885 13.181 31.82s-4.741 23.38-13.181 31.82L338.838 961.376c-17.574 17.573-46.065 17.573-63.64-0.001-17.573-17.573-17.573-46.065 0.001-63.64L660.944 512 275.198 126.265c-17.574-17.573-17.574-46.066-0.001-63.64C283.985 53.839 295.501 49.445 307.018 49.445z"
}, null, -1);

var ContextMenuIconRightvue_type_template_id_41a351a6_hoisted_3 = [ContextMenuIconRightvue_type_template_id_41a351a6_hoisted_2];
function ContextMenuIconRightvue_type_template_id_41a351a6_render(_ctx, _cache) {
  return (0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)("svg", ContextMenuIconRightvue_type_template_id_41a351a6_hoisted_1, ContextMenuIconRightvue_type_template_id_41a351a6_hoisted_3);
}
;// CONCATENATED MODULE: ./src/ContextMenuIconRight.vue?vue&type=template&id=41a351a6

;// CONCATENATED MODULE: ./src/ContextMenuIconRight.vue

const ContextMenuIconRight_script = {}

;
const ContextMenuIconRight_exports_ = /*#__PURE__*/(0,exportHelper/* default */.Z)(ContextMenuIconRight_script, [['render',ContextMenuIconRightvue_type_template_id_41a351a6_render]])

/* harmony default export */ const ContextMenuIconRight = (ContextMenuIconRight_exports_);
;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js!./node_modules/ts-loader/index.js??clonedRuleSet-86.use[2]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextMenuItem.vue?vue&type=script&lang=ts




/**
 * Menu Item
 */

/* harmony default export */ const ContextMenuItemvue_type_script_lang_ts = ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.defineComponent)({
  name: 'ContextMenuItem',
  components: {
    VNodeRender: VNodeRender,
    ContextMenuIconCheck: ContextMenuIconCheck,
    ContextMenuIconRight: ContextMenuIconRight
  },
  props: {
    /**
     * Is this menu disabled?
     */
    disabled: {
      type: Boolean,
      "default": false
    },

    /**
     * Is this menu hidden?
     */
    hidden: {
      type: Boolean,
      "default": false
    },
    customRender: {
      type: Function,
      "default": null
    },

    /**
     * Custom css class for submenu
     */
    customClass: {
      type: String,
      "default": ''
    },
    clickHandler: {
      type: Function,
      "default": null
    },

    /**
     * Menu label
     */
    label: {
      type: [String, Object, Function],
      "default": ''
    },

    /**
     * Menu icon (for icon class)
     */
    icon: {
      type: [String, Object, Function],
      "default": ''
    },

    /**
     * Custom icon library font class name.
     *
     * Only for css font icon, If you use the svg icon, you do not need to use this.
     */
    iconFontClass: {
      type: String,
      "default": 'iconfont'
    },

    /**
     * Is this menu item checked?
     *
     * The check mark are displayed on the left side of the icon, so it is not recommended to display the icon at the same time.
     */
    checked: {
      type: Boolean,
      "default": false
    },

    /**
     * Shortcut key text display on the right.
     *
     * The shortcut keys here are only for display. You need to handle the key events by yourself.
     */
    shortcut: {
      type: String,
      "default": ''
    },

    /**
     * Display icons use svg symbol (`<use xlink:href="#icon-symbol-name">`) ， only valid when icon attribute is empty.
     */
    svgIcon: {
      type: String,
      "default": ''
    },

    /**
     * The user-defined attribute of the svg tag, which is valid when using `svgIcon`.
     */
    svgProps: {
      type: Object,
      "default": null
    },

    /**
     * Should a fixed-width icon area be reserved for menu items without icon. (this item)
     *
     * Default is true .
     *
     * The width of icon area can be override with css var `--mx-menu-placeholder-width`.
     */
    preserveIconWidth: {
      type: Boolean,
      "default": true
    },

    /**
     * Show right arrow on this menu?
     */
    showRightArrow: {
      type: Boolean,
      "default": false
    },
    hasChildren: {
      type: Boolean,
      "default": false
    },

    /**
     * Should close menu when Click this menu item ?
     */
    clickClose: {
      type: Boolean,
      "default": true
    },

    /**
     * When there are subitems in this item, is it allowed to trigger its own click event? Default is false
     */
    clickableWhenHasChildren: {
      type: Boolean,
      "default": false
    }
  },
  emits: ['click'],
  setup: function setup(props, context) {
    var _toRefs = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.toRefs)(props),
        clickHandler = _toRefs.clickHandler,
        clickClose = _toRefs.clickClose,
        clickableWhenHasChildren = _toRefs.clickableWhenHasChildren,
        disabled = _toRefs.disabled,
        hidden = _toRefs.hidden,
        label = _toRefs.label,
        icon = _toRefs.icon,
        iconFontClass = _toRefs.iconFontClass,
        showRightArrow = _toRefs.showRightArrow,
        shortcut = _toRefs.shortcut,
        hasChildren = _toRefs.hasChildren;

    var _showSubMenu = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)(false);

    var keyBoardFocusMenu = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)(false);
    var menuItemRef = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)();
    var globalHasSlot = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalHasSlot');
    var globalRenderSlot = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalRenderSlot');
    var globalTheme = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalTheme');
    var globalIconFontClass = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalIconFontClass');
    var globalCloseMenu = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalCloseMenu');
    var menuContext = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('menuContext'); //Instance Contet for keyboadr control

    var menuItemInstance = {
      showSubMenu: function showSubMenu() {
        if (_showSubMenu.value) {
          //Mark current item
          menuContext.markActiveMenuItem(menuItemInstance, true);
        } else if (hasChildren.value) onMouseEnter();
      },
      isDisabledOrHidden: function isDisabledOrHidden() {
        return disabled.value || hidden.value;
      },
      getElement: function getElement() {
        return menuItemRef.value;
      },
      focus: function focus() {
        return keyBoardFocusMenu.value = true;
      },
      blur: function blur() {
        return keyBoardFocusMenu.value = false;
      },
      click: onClick
    };
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.onMounted)(function () {
      if (menuContext.isMenuItemDataCollectedFlag()) {
        //当前菜单条目是在整体加载完成后才显示的，此时菜单顺序已经无法知道，
        //所以这里需要在父级元素中查找得出当前菜单的位置。
        //
        //The current menu item is displayed after the overall loading is completed. 
        //At this time, the menu order cannot be known, so here we need to 
        //find the position of the current menu in the parent element.
        (0,external_commonjs_vue_commonjs2_vue_root_Vue_.nextTick)(function () {
          var index = 0;
          var parentEl = menuContext.getElement();

          if (parentEl) {
            var indexCounting = 0;

            for (var i = 0; i < parentEl.children.length; i++) {
              var el = parentEl.children[i];

              if (el.getAttribute('data-type') === 'ContextMenuItem') {
                if (el === menuItemRef.value) {
                  index = indexCounting;
                  break;
                }

                indexCounting++;
              }
            }
          } //Insert to pos


          menuContext.addChildMenuItem(menuItemInstance, index);
        });
      } else menuContext.addChildMenuItem(menuItemInstance);
    });
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.onBeforeUnmount)(function () {
      menuContext.removeChildMenuItem(menuItemInstance);
    }); //Click handler

    function onClick(e) {
      //Ignore clicking when disabled or click on some special elements
      if (disabled.value || e && e.target.classList.contains('mx-context-no-clickable')) return; //Has submenu?

      if (hasChildren.value) {
        if (clickableWhenHasChildren.value) {
          if (typeof clickHandler.value === 'function') clickHandler.value();
          context.emit('click');
        } else if (!_showSubMenu.value) onMouseEnter();
      } else {
        //Call hander from options
        if (typeof clickHandler.value === 'function') clickHandler.value();
        context.emit('click');

        if (clickClose.value) {
          //emit close
          globalCloseMenu();
        }
      }
    } //MouseEnter handler: show item submenu


    function onMouseEnter(e) {
      //Clear keyBoard focus style
      keyBoardFocusMenu.value = false; //等待一个延时，以防止用户过快移动鼠标导致菜单隐藏
      //Wait for a delay to prevent the menu from being hidden due to the user moving the mouse too fast

      if (!menuContext.checkCloseOtherSubMenuTimeOut()) menuContext.closeOtherSubMenu();

      if (!disabled.value) {
        //Mark current item
        menuContext.markActiveMenuItem(menuItemInstance);

        if (hasChildren.value) {
          if (!e) menuContext.markThisOpenedByKeyBoard(); //Open sub menu

          menuContext.addOpenedSubMenu(function () {
            keyBoardFocusMenu.value = false;
            _showSubMenu.value = false;
          });
          _showSubMenu.value = true;
        }
      }
    }

    return {
      //Data for custom render
      getItemDataForChildren: function getItemDataForChildren() {
        return {
          disabled: disabled.value,
          label: label.value,
          icon: icon.value,
          iconFontClass: iconFontClass.value,
          showRightArrow: showRightArrow.value,
          clickClose: clickClose.value,
          clickableWhenHasChildren: clickableWhenHasChildren.value,
          shortcut: shortcut.value,
          theme: globalTheme,
          isOpen: _showSubMenu,
          hasChildren: hasChildren,
          onClick: onClick,
          onMouseEnter: onMouseEnter
        };
      },
      showSubMenu: _showSubMenu,
      keyBoardFocusMenu: keyBoardFocusMenu,
      menuItemRef: menuItemRef,
      globalHasSlot: globalHasSlot,
      globalRenderSlot: globalRenderSlot,
      globalIconFontClass: globalIconFontClass,
      onMouseEnter: onMouseEnter,
      onClick: onClick
    };
  }
}));
;// CONCATENATED MODULE: ./src/ContextMenuItem.vue?vue&type=script&lang=ts
 
;// CONCATENATED MODULE: ./src/ContextMenuItem.vue




;
const ContextMenuItem_exports_ = /*#__PURE__*/(0,exportHelper/* default */.Z)(ContextMenuItemvue_type_script_lang_ts, [['render',ContextMenuItemvue_type_template_id_027b05a5_ts_true_render]])

/* harmony default export */ const ContextMenuItem = (ContextMenuItem_exports_);
;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js!./node_modules/ts-loader/index.js??clonedRuleSet-86.use[2]!./node_modules/vue-loader/dist/templateLoader.js??ruleSet[1].rules[4]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextMenuSeparator.vue?vue&type=template&id=27643a0a&ts=true

var ContextMenuSeparatorvue_type_template_id_27643a0a_ts_true_hoisted_1 = {
  "class": "mx-context-menu-item-sperator mx-context-no-clickable"
};
function ContextMenuSeparatorvue_type_template_id_27643a0a_ts_true_render(_ctx, _cache, $props, $setup, $data, $options) {
  return (0,external_commonjs_vue_commonjs2_vue_root_Vue_.openBlock)(), (0,external_commonjs_vue_commonjs2_vue_root_Vue_.createElementBlock)("div", ContextMenuSeparatorvue_type_template_id_27643a0a_ts_true_hoisted_1);
}
;// CONCATENATED MODULE: ./src/ContextMenuSeparator.vue?vue&type=template&id=27643a0a&ts=true

;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js!./node_modules/ts-loader/index.js??clonedRuleSet-86.use[2]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextMenuSeparator.vue?vue&type=script&lang=ts

/* harmony default export */ const ContextMenuSeparatorvue_type_script_lang_ts = ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.defineComponent)({
  name: 'ContextMenuSperator'
}));
;// CONCATENATED MODULE: ./src/ContextMenuSeparator.vue?vue&type=script&lang=ts
 
;// CONCATENATED MODULE: ./src/ContextMenuSeparator.vue




;
const ContextMenuSeparator_exports_ = /*#__PURE__*/(0,exportHelper/* default */.Z)(ContextMenuSeparatorvue_type_script_lang_ts, [['render',ContextMenuSeparatorvue_type_template_id_27643a0a_ts_true_render]])

/* harmony default export */ const ContextMenuSeparator = (ContextMenuSeparator_exports_);
;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js!./node_modules/ts-loader/index.js??clonedRuleSet-86.use[2]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextSubMenu.vue?vue&type=script&lang=ts













/**
 * Submenu container
 */

/* harmony default export */ const ContextSubMenuvue_type_script_lang_ts = ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.defineComponent)({
  name: 'ContextSubMenu',
  components: {
    ContextMenuItem: ContextMenuItem,
    ContextMenuSeparator: ContextMenuSeparator,
    VNodeRender: VNodeRender,
    ContextMenuIconRight: ContextMenuIconRight
  },
  props: {
    /**
     * Items from options
     */
    items: {
      type: Object,
      "default": null
    },

    /**
     * Max width for this submenu
     */
    maxWidth: {
      type: [String, Number],
      "default": 0
    },

    /**
     * Min width for this submenu
     */
    minWidth: {
      type: [String, Number],
      "default": 0
    },

    /**
     * Specifies should submenu adjust it position
     * when the menu exceeds the screen. The default is true
     */
    adjustPosition: {
      type: Boolean,
      "default": true
    }
  },
  setup: function setup(props) {
    //#region Injects
    var parentContext = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('menuContext');
    var options = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalOptions');
    var globalHasSlot = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalHasSlot');
    var globalRenderSlot = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalRenderSlot');
    var globalTheme = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalTheme');
    var globalIsFullScreenContainer = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalIsFullScreenContainer'); //#endregion

    var zIndex = parentContext.zIndex,
        getMyPosition = parentContext.getMyPosition,
        getParentWidth = parentContext.getParentWidth;

    var _toRefs = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.toRefs)(props),
        adjustPosition = _toRefs.adjustPosition;

    var menu = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)();
    var scroll = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)();
    var openedSubMenuClose = []; //#region Keyboard control context

    var globalSetCurrentSubMenu = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalSetCurrentSubMenu');
    var menuItems = [];
    var currentItem = null;
    var leaveTimeout = 0;

    function blurCurrentMenu() {
      if (currentItem) currentItem.blur();
    }

    function setAndFocusNotDisableItem(isDown, startIndex) {
      if (isDown) {
        for (var i = startIndex !== undefined ? startIndex : 0; i < menuItems.length; i++) {
          if (!menuItems[i].isDisabledOrHidden()) {
            setAndFocusCurrentMenu(i);
            break;
          }
        }
      } else {
        for (var _i = startIndex !== undefined ? startIndex : menuItems.length - 1; _i >= 0; _i--) {
          if (!menuItems[_i].isDisabledOrHidden()) {
            setAndFocusCurrentMenu(_i);
            break;
          }
        }
      }
    }

    function setAndFocusCurrentMenu(index) {
      if (currentItem) blurCurrentMenu();
      if (index !== undefined) currentItem = menuItems[Math.max(0, Math.min(index, menuItems.length - 1))];
      if (!currentItem) return; //Focus item

      currentItem.focus(); //Scroll to current item

      if (overflow.value) {
        var element = currentItem.getElement();

        if (element) {
          scrollValue.value = Math.min(Math.max(-scrollHeight.value, -element.offsetTop - element.offsetHeight + maxHeight.value), 0);
        }
      }
    }

    function onSubMenuBodyClick() {
      //Mouse click can set current focused submenu 
      globalSetCurrentSubMenu(thisMenuInsContext);
    }

    var thisMenuInsContext = {
      isTopLevel: function isTopLevel() {
        return parentContext.getParentContext() === null;
      },
      closeSelfAndActiveParent: function closeSelfAndActiveParent() {
        var parent = thisMenuContext.getParentContext();

        if (parent) {
          var _parent$getSubMenuIns;

          parent.closeOtherSubMenu();
          (_parent$getSubMenuIns = parent.getSubMenuInstanceContext()) === null || _parent$getSubMenuIns === void 0 ? void 0 : _parent$getSubMenuIns.focusCurrentItem();
        }
      },
      closeCurrentSubMenu: function closeCurrentSubMenu() {
        var _thisMenuContext$getP;

        return (_thisMenuContext$getP = thisMenuContext.getParentContext()) === null || _thisMenuContext$getP === void 0 ? void 0 : _thisMenuContext$getP.closeOtherSubMenu();
      },
      moveCurrentItemFirst: function moveCurrentItemFirst() {
        return setAndFocusNotDisableItem(true);
      },
      moveCurrentItemLast: function moveCurrentItemLast() {
        return setAndFocusNotDisableItem(false);
      },
      moveCurrentItemDown: function moveCurrentItemDown() {
        return setAndFocusNotDisableItem(true, currentItem ? menuItems.indexOf(currentItem) + 1 : 0);
      },
      moveCurrentItemUp: function moveCurrentItemUp() {
        return setAndFocusNotDisableItem(false, currentItem ? menuItems.indexOf(currentItem) - 1 : 0);
      },
      focusCurrentItem: function focusCurrentItem() {
        return setAndFocusCurrentMenu();
      },
      openCurrentItemSubMenu: function openCurrentItemSubMenu() {
        var _currentItem;

        return (_currentItem = currentItem) === null || _currentItem === void 0 ? void 0 : _currentItem.showSubMenu();
      },
      triggerCurrentItemClick: function triggerCurrentItemClick() {
        var _currentItem2;

        return (_currentItem2 = currentItem) === null || _currentItem2 === void 0 ? void 0 : _currentItem2.click();
      }
    };
    var _isOpenedByKeyBoardFlag = false;
    var _isMenuItemDataCollectedFlag = false; //#endregion
    //#region Menu control context
    //provide menuContext for child use

    var thisMenuContext = {
      zIndex: zIndex + 1,
      container: parentContext.container,
      adjustPadding: parentContext.adjustPadding,
      getMyPosition: function getMyPosition() {
        var pos = {
          x: 0,
          y: 0
        }; //计算子菜单的位置

        if (menu.value) pos.x = menu.value.offsetWidth + (options.xOffset || 0);
        if (options.yOffset) pos.y = options.yOffset;
        return pos;
      },
      getParentWidth: function getParentWidth() {
        var _menu$value;

        return ((_menu$value = menu.value) === null || _menu$value === void 0 ? void 0 : _menu$value.offsetWidth) || 0;
      },
      getParentAbsY: function getParentAbsY() {
        return menu.value ? getTop(menu.value, parentContext.container) : 0;
      },
      addOpenedSubMenu: function addOpenedSubMenu(closeFn) {
        openedSubMenuClose.push(closeFn);
      },
      closeOtherSubMenu: function closeOtherSubMenu() {
        openedSubMenuClose.forEach(function (k) {
          return k();
        });
        openedSubMenuClose.splice(0, openedSubMenuClose.length);
        globalSetCurrentSubMenu(thisMenuInsContext);
      },
      checkCloseOtherSubMenuTimeOut: function checkCloseOtherSubMenuTimeOut() {
        if (leaveTimeout) {
          clearTimeout(leaveTimeout);
          leaveTimeout = 0;
          return true;
        }

        return false;
      },
      closeOtherSubMenuWithTimeOut: function closeOtherSubMenuWithTimeOut() {
        var _this = this;

        leaveTimeout = setTimeout(function () {
          leaveTimeout = 0;

          _this.closeOtherSubMenu();
        }, 200); //Add a delay, the user will not hide the menu when moving too fast
      },
      addChildMenuItem: function addChildMenuItem(item, index) {
        if (index === undefined) menuItems.push(item);else menuItems.splice(index, 0, item);
      },
      removeChildMenuItem: function removeChildMenuItem(item) {
        menuItems.splice(menuItems.indexOf(item), 1);
      },
      markActiveMenuItem: function markActiveMenuItem(item) {
        var updateState = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : false;
        blurCurrentMenu();
        currentItem = item;
        if (updateState) setAndFocusCurrentMenu();
      },
      markThisOpenedByKeyBoard: function markThisOpenedByKeyBoard() {
        _isOpenedByKeyBoardFlag = true;
      },
      isOpenedByKeyBoardFlag: function isOpenedByKeyBoardFlag() {
        if (_isOpenedByKeyBoardFlag) {
          _isOpenedByKeyBoardFlag = false;
          return true;
        }

        return false;
      },
      isMenuItemDataCollectedFlag: function isMenuItemDataCollectedFlag() {
        return _isMenuItemDataCollectedFlag;
      },
      getElement: function getElement() {
        return menu.value || null;
      },
      getParentContext: function getParentContext() {
        return parentContext;
      },
      getSubMenuInstanceContext: function getSubMenuInstanceContext() {
        return thisMenuInsContext;
      }
    };
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('menuContext', thisMenuContext); //#endregion

    var scrollValue = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)(0);
    var scrollHeight = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)(0); //Scroll the items

    function onScroll(down) {
      if (down) scrollValue.value = Math.max(scrollValue.value - 50, -scrollHeight.value);else scrollValue.value = Math.min(scrollValue.value + 50, 0);
    }

    var overflow = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)(false);
    var position = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)({
      x: 0,
      y: 0
    });
    var maxHeight = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)(0);
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.onMounted)(function () {
      position.value = getMyPosition(); //Mark current item submenu is open

      globalSetCurrentSubMenu(thisMenuInsContext);
      (0,external_commonjs_vue_commonjs2_vue_root_Vue_.nextTick)(function () {
        var menuEl = menu.value; //adjust submenu position

        if (adjustPosition.value && menuEl && scroll.value) {
          var container = parentContext.container,
              fillPadding = parentContext.adjustPadding;
          var windowHeight = document.documentElement.scrollHeight;
          var windowWidth = document.documentElement.scrollWidth;
          var avliableWidth = Math.min(windowWidth, container.offsetWidth);
          var avliableHeight = Math.min(windowHeight, container.offsetHeight);
          var absX = getLeft(menuEl, container),
              absY = getTop(menuEl, container);
          var xOverflow = absX + menuEl.offsetWidth - avliableWidth;
          var yOverflow = absY + menuEl.offsetHeight - avliableHeight;
          scrollHeight.value = menuEl.offsetHeight - avliableHeight + fillPadding * 2
          /* Padding */
          ;
          overflow.value = yOverflow > 0;
          if (xOverflow > 0) //X overflow
            position.value.x -= (getParentWidth ? getParentWidth() : 0) + menuEl.offsetWidth - fillPadding;

          if (overflow.value) {
            var oy = Math.min(absY - fillPadding, yOverflow + fillPadding);
            position.value.y -= oy;
            maxHeight.value = avliableHeight - fillPadding * 2;
          } else {
            maxHeight.value = 0;
          }
        } //Focus this submenu


        menuEl === null || menuEl === void 0 ? void 0 : menuEl.focus({
          preventScroll: true
        }); //Is this submenu opened by keyboard? If yes then select first item

        if (parentContext.isOpenedByKeyBoardFlag()) setAndFocusNotDisableItem(true);
        _isMenuItemDataCollectedFlag = true;
      });
    });
    return {
      menu: menu,
      scroll: scroll,
      options: options,
      zIndex: zIndex,
      constOptions: MenuConstOptions,
      scrollValue: scrollValue,
      overflow: overflow,
      position: position,
      scrollHeight: scrollHeight,
      maxHeight: maxHeight,
      globalHasSlot: globalHasSlot,
      globalRenderSlot: globalRenderSlot,
      globalTheme: globalTheme,
      onScroll: onScroll,
      onSubMenuBodyClick: onSubMenuBodyClick
    };
  }
}));
;// CONCATENATED MODULE: ./src/ContextSubMenu.vue?vue&type=script&lang=ts
 
;// CONCATENATED MODULE: ./node_modules/mini-css-extract-plugin/dist/loader.js??clonedRuleSet-67.use[0]!./node_modules/css-loader/dist/cjs.js??clonedRuleSet-67.use[1]!./node_modules/vue-loader/dist/stylePostLoader.js!./node_modules/postcss-loader/dist/cjs.js??clonedRuleSet-67.use[2]!./node_modules/sass-loader/dist/cjs.js??clonedRuleSet-67.use[3]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextSubMenu.vue?vue&type=style&index=0&id=60fc14ea&lang=scss
// extracted by mini-css-extract-plugin

;// CONCATENATED MODULE: ./src/ContextSubMenu.vue?vue&type=style&index=0&id=60fc14ea&lang=scss

;// CONCATENATED MODULE: ./src/ContextSubMenu.vue




;


const ContextSubMenu_exports_ = /*#__PURE__*/(0,exportHelper/* default */.Z)(ContextSubMenuvue_type_script_lang_ts, [['render',render]])

/* harmony default export */ const ContextSubMenu = (ContextSubMenu_exports_);
;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js!./node_modules/ts-loader/index.js??clonedRuleSet-86.use[2]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextSubMenuWrapper.vue?vue&type=script&lang=ts






/**
 * Context menu component
 */

/* harmony default export */ const ContextSubMenuWrappervue_type_script_lang_ts = ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.defineComponent)({
  name: 'ContextMenu',
  emits: ['update:show', 'close'],
  props: {
    /**
     * Menu options
     */
    options: {
      type: Object,
      "default": null
    },

    /**
     * Show menu?
     */
    show: {
      type: Boolean,
      "default": false
    },

    /**
     * Current container, For calculation only
     */
    container: {
      type: Object,
      "default": null
    },

    /**
     * Make sure is user set the custom container.
     */
    isFullScreenContainer: {
      type: Boolean,
      "default": true
    }
  },
  setup: function setup(props, ctx) {
    var _options$value, _options$value2;

    var _toRefs = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.toRefs)(props),
        options = _toRefs.options,
        show = _toRefs.show,
        container = _toRefs.container;

    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.onMounted)(function () {
      if (show) openMenu();
    });
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.onBeforeUnmount)(function () {
      removeBodyEvents();
    });
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.watch)(show, function (v) {
      if (v) {
        openMenu();
      } else {
        removeBodyEvents();
      }
    });
    var instance = {
      closeMenu: closeMenu
    };

    function openMenu() {
      installBodyEvents();
      addOpenedContextMenu(instance);
    }

    function closeMenu() {
      ctx.emit("update:show", false);
      ctx.emit("close");
      removeOpenedContextMenu(instance);
    } //Expose instance function


    ctx.expose({
      closeMenu: closeMenu
    });

    function installBodyEvents() {
      setTimeout(function () {
        document.addEventListener("click", onBodyClick, true);
        document.addEventListener("contextmenu", onBodyClick, true);
        document.addEventListener("wheel", onBodyWhell, true);
        if (options.value.keyboardControl !== false) document.addEventListener('keydown', onMenuKeyDown);
      }, 50);
    }

    function removeBodyEvents() {
      document.removeEventListener("contextmenu", onBodyClick, true);
      document.removeEventListener("click", onBodyClick, true);
      document.removeEventListener("wheel", onBodyWhell, true);
      if (options.value.keyboardControl !== false) document.removeEventListener('keydown', onMenuKeyDown);
    } //For keyboard event, remember which submenu is active


    var currentOpenedMenu = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.ref)();
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('globalSetCurrentSubMenu', function (menu) {
      return currentOpenedMenu.value = menu;
    });

    function onMenuKeyDown(e) {
      var _currentOpenedMenu$va3, _currentOpenedMenu$va4, _currentOpenedMenu$va5, _currentOpenedMenu$va6, _currentOpenedMenu$va8, _currentOpenedMenu$va9;

      var handled = true; //Handle keyboard event

      switch (e.key) {
        case "Escape":
          {
            var _currentOpenedMenu$va;

            if (((_currentOpenedMenu$va = currentOpenedMenu.value) === null || _currentOpenedMenu$va === void 0 ? void 0 : _currentOpenedMenu$va.isTopLevel()) === false) {
              var _currentOpenedMenu$va2;

              (_currentOpenedMenu$va2 = currentOpenedMenu.value) === null || _currentOpenedMenu$va2 === void 0 ? void 0 : _currentOpenedMenu$va2.closeCurrentSubMenu();
            } else {
              closeMenu();
            }

            break;
          }

        case "ArrowDown":
          (_currentOpenedMenu$va3 = currentOpenedMenu.value) === null || _currentOpenedMenu$va3 === void 0 ? void 0 : _currentOpenedMenu$va3.moveCurrentItemDown();
          break;

        case "ArrowUp":
          (_currentOpenedMenu$va4 = currentOpenedMenu.value) === null || _currentOpenedMenu$va4 === void 0 ? void 0 : _currentOpenedMenu$va4.moveCurrentItemUp();
          break;

        case "Home":
          (_currentOpenedMenu$va5 = currentOpenedMenu.value) === null || _currentOpenedMenu$va5 === void 0 ? void 0 : _currentOpenedMenu$va5.moveCurrentItemFirst();
          break;

        case "End":
          (_currentOpenedMenu$va6 = currentOpenedMenu.value) === null || _currentOpenedMenu$va6 === void 0 ? void 0 : _currentOpenedMenu$va6.moveCurrentItemLast();
          break;

        case "ArrowLeft":
          {
            var _currentOpenedMenu$va7;

            (_currentOpenedMenu$va7 = currentOpenedMenu.value) === null || _currentOpenedMenu$va7 === void 0 ? void 0 : _currentOpenedMenu$va7.closeSelfAndActiveParent();
            break;
          }

        case "ArrowRight":
          (_currentOpenedMenu$va8 = currentOpenedMenu.value) === null || _currentOpenedMenu$va8 === void 0 ? void 0 : _currentOpenedMenu$va8.openCurrentItemSubMenu();
          break;

        case "Enter":
          (_currentOpenedMenu$va9 = currentOpenedMenu.value) === null || _currentOpenedMenu$va9 === void 0 ? void 0 : _currentOpenedMenu$va9.triggerCurrentItemClick();
          break;

        default:
          handled = false;
          break;
      }

      if (handled && currentOpenedMenu.value) {
        e.stopPropagation();
        e.preventDefault();
      }
    }

    function onBodyWhell() {
      //close when mouse scroll
      if (options.value.closeWhenScroll !== false) closeMenu();
    }

    function onBodyClick(e) {
      checkTargetAndClose(e.target);
    }

    function checkTargetAndClose(target) {
      //Loop target , Check whether the currently clicked element belongs to the current menu.
      // If yes, it will not be closed
      while (target) {
        if (target.classList && target.classList.contains('mx-menu-host')) return;
        target = target.parentNode;
      } //Close menu


      removeBodyEvents();
      closeMenu();
    } //provide globalOptions for child use


    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('globalOptions', options.value);
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('globalCloseMenu', closeMenu);
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('globalTheme', ((_options$value = options.value) === null || _options$value === void 0 ? void 0 : _options$value.theme) || 'light');
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('globalIsFullScreenContainer', props.isFullScreenContainer);
    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('globalIconFontClass', ((_options$value2 = options.value) === null || _options$value2 === void 0 ? void 0 : _options$value2.iconFontClass) || 'iconfont'); //check slot exists

    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('globalHasSlot', function (name) {
      return ctx.slots[name] !== undefined;
    }); //render slot

    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('globalRenderSlot', function (name, params) {
      return (0,external_commonjs_vue_commonjs2_vue_root_Vue_.renderSlot)(ctx.slots, name, _objectSpread2({}, params));
    }); //provide menuContext for child use

    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.provide)('menuContext', {
      zIndex: options.value.zIndex || MenuConstOptions.defaultZindex,
      container: container.value,
      adjustPadding: options.value.adjustPadding || MenuConstOptions.defaultAdjustPadding,
      getParentAbsY: function getParentAbsY() {
        return options.value.y;
      },
      getMyPosition: function getMyPosition() {
        return {
          x: options.value.x,
          y: options.value.y
        };
      },
      getParentWidth: function getParentWidth() {
        return 0;
      },
      closeOtherSubMenuWithTimeOut: function closeOtherSubMenuWithTimeOut() {},
      checkCloseOtherSubMenuTimeOut: function checkCloseOtherSubMenuTimeOut() {
        return false;
      },
      addOpenedSubMenu: function addOpenedSubMenu() {},
      closeOtherSubMenu: function closeOtherSubMenu() {},
      getParentContext: function getParentContext() {
        return null;
      },
      getSubMenuInstanceContext: function getSubMenuInstanceContext() {
        return null;
      },
      getElement: function getElement() {
        return null;
      },
      addChildMenuItem: function addChildMenuItem() {},
      removeChildMenuItem: function removeChildMenuItem() {},
      markActiveMenuItem: function markActiveMenuItem() {},
      markThisOpenedByKeyBoard: function markThisOpenedByKeyBoard() {},
      isOpenedByKeyBoardFlag: function isOpenedByKeyBoardFlag() {
        return false;
      },
      isMenuItemDataCollectedFlag: function isMenuItemDataCollectedFlag() {
        return false;
      }
    });
    return function () {
      var _options$value3, _options$value4;

      //Hidden
      if (!show.value) return []; //Create SubMenu

      return [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.h)('div', {
        "class": 'mx-menu-ghost-host'
      }, [(0,external_commonjs_vue_commonjs2_vue_root_Vue_.h)(ContextSubMenu, {
        "class": 'mx-menu-host',
        items: (_options$value3 = options.value) === null || _options$value3 === void 0 ? void 0 : _options$value3.items,
        adjustPosition: (_options$value4 = options.value) === null || _options$value4 === void 0 ? void 0 : _options$value4.adjustPosition,
        maxWidth: options.value.maxWidth || MenuConstOptions.defaultMaxWidth,
        minWidth: options.value.minWidth || MenuConstOptions.defaultMinWidth
      }, {
        "default": ctx.slots["default"]
      })])];
    };
  }
}));
;// CONCATENATED MODULE: ./src/ContextSubMenuWrapper.vue?vue&type=script&lang=ts
 
;// CONCATENATED MODULE: ./node_modules/mini-css-extract-plugin/dist/loader.js??clonedRuleSet-57.use[0]!./node_modules/css-loader/dist/cjs.js??clonedRuleSet-57.use[1]!./node_modules/vue-loader/dist/stylePostLoader.js!./node_modules/postcss-loader/dist/cjs.js??clonedRuleSet-57.use[2]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextSubMenuWrapper.vue?vue&type=style&index=0&id=533580ac&lang=css
// extracted by mini-css-extract-plugin

;// CONCATENATED MODULE: ./src/ContextSubMenuWrapper.vue?vue&type=style&index=0&id=533580ac&lang=css

;// CONCATENATED MODULE: ./src/ContextSubMenuWrapper.vue



;

const ContextSubMenuWrapper_exports_ = ContextSubMenuWrappervue_type_script_lang_ts;

/* harmony default export */ const ContextSubMenuWrapper = (ContextSubMenuWrapper_exports_);
;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js!./node_modules/ts-loader/index.js??clonedRuleSet-86.use[2]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextMenu.vue?vue&type=script&lang=ts



/**
 * Context menu component
 */

/* harmony default export */ const ContextMenuvue_type_script_lang_ts = ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.defineComponent)({
  name: 'ContextMenu',
  emits: ['update:show', 'close'],
  props: {
    /**
     * Menu options
     */
    options: {
      type: Object,
      "default": null
    },

    /**
     * Show menu?
     */
    show: {
      type: Boolean,
      "default": false
    }
  },
  setup: function setup(props, ctx) {
    var _toRefs = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.toRefs)(props),
        options = _toRefs.options,
        show = _toRefs.show;

    var currentContainer = null;
    var currentContainerIsNew = false;

    function openMenu() {
      var _genContainer = genContainer(options.value),
          container = _genContainer.container,
          isNew = _genContainer.isNew;

      var vnode = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.h)(ContextSubMenuWrapper, {
        options: options,
        show: true,
        container: container,
        isFullScreenContainer: !isNew,
        'onUpdate:show': function onUpdateShow(v) {
          return ctx.emit('update:show', v);
        },
        onClose: function onClose() {
          (0,external_commonjs_vue_commonjs2_vue_root_Vue_.render)(null, container);
          ctx.emit('close');
        }
      }, ctx.slots);
      currentContainerIsNew = isNew;
      currentContainer = container;
      (0,external_commonjs_vue_commonjs2_vue_root_Vue_.render)(vnode, container);
    }

    function closeMenu() {
      if (currentContainer) {
        if (currentContainerIsNew) removeContainer(currentContainer);
        currentContainer = null;
      }
    }

    (0,external_commonjs_vue_commonjs2_vue_root_Vue_.watch)(show, function (v) {
      if (v) openMenu();else closeMenu();
    }); //watch(() => options.value.x, () => checkAndRecreate());
    //watch(() => options.value.y, () => checkAndRecreate());

    ctx.expose({
      closeMenu: closeMenu
    });
    return function () {
      return [];
    };
  }
}));
;// CONCATENATED MODULE: ./src/ContextMenu.vue?vue&type=script&lang=ts
 
;// CONCATENATED MODULE: ./src/ContextMenu.vue



const ContextMenu_exports_ = ContextMenuvue_type_script_lang_ts;

/* harmony default export */ const ContextMenu = (ContextMenu_exports_);
;// CONCATENATED MODULE: ./node_modules/@babel/runtime/helpers/esm/typeof.js
function _typeof(obj) {
  "@babel/helpers - typeof";

  return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (obj) {
    return typeof obj;
  } : function (obj) {
    return obj && "function" == typeof Symbol && obj.constructor === Symbol && obj !== Symbol.prototype ? "symbol" : typeof obj;
  }, _typeof(obj);
}
;// CONCATENATED MODULE: ./node_modules/thread-loader/dist/cjs.js!./node_modules/babel-loader/lib/index.js!./node_modules/ts-loader/index.js??clonedRuleSet-86.use[2]!./node_modules/vue-loader/dist/index.js??ruleSet[0].use[0]!./src/ContextMenuGroup.vue?vue&type=script&lang=ts






/* harmony default export */ const ContextMenuGroupvue_type_script_lang_ts = ((0,external_commonjs_vue_commonjs2_vue_root_Vue_.defineComponent)({
  name: 'ContextMenuGroup',
  props: {
    /**
    * Is this menu disabled?
    */
    disabled: {
      type: Boolean,
      "default": false
    },

    /**
    * Is this menu hidden?
    */
    hidden: {
      type: Boolean,
      "default": false
    },

    /**
     * Is this menu disabled?
     */
    clickHandler: {
      type: Function,
      "default": null
    },

    /**
     * Menu label
     */
    label: {
      type: String,
      "default": ''
    },

    /**
     * Menu icon (for icon class)
     */
    icon: {
      type: String,
      "default": ''
    },

    /**
     * Custom icon library font class name.
     *
     * Only for css font icon, If you use the svg icon, you do not need to use this.
     */
    iconFontClass: {
      type: String,
      "default": 'iconfont'
    },

    /**
     * Is this menu item checked?
     *
     * The check mark are displayed on the left side of the icon, so it is not recommended to display the icon at the same time.
     */
    checked: {
      type: Boolean,
      "default": false
    },

    /**
     * Shortcut key text display on the right.
     *
     * The shortcut keys here are only for display. You need to handle the key events by yourself.
     */
    shortcut: {
      type: String,
      "default": ''
    },

    /**
     * Display icons use svg symbol (`<use xlink:href="#icon-symbol-name">`) ， only valid when icon attribute is empty.
     */
    svgIcon: {
      type: String,
      "default": ''
    },

    /**
     * The user-defined attribute of the svg tag, which is valid when using `svgIcon`.
     */
    svgProps: {
      type: Object,
      "default": null
    },

    /**
     * Should a fixed-width icon area be reserved for menu items without icon. (this item)
     *
     * Default is true .
     *
     * The width of icon area can be override with css var `--mx-menu-placeholder-width`.
     */
    preserveIconWidth: {
      type: Boolean,
      "default": true
    },

    /**
     * Show right arrow on this menu?
     */
    showRightArrow: {
      type: Boolean,
      "default": false
    },

    /**
     * Should close menu when Click this menu item ?
     */
    clickClose: {
      type: Boolean,
      "default": true
    },

    /**
     * By default, the submenu will automatically adjust its position to prevent it overflow the container.
     *
     * If you allow menu overflow containers, you can set this to false.
     *
     * Default is inherit from `MenuOptions.adjustPosition`  .
     */
    adjustSubMenuPosition: {
      type: Boolean,
      "default": undefined
    },

    /**
     * Max width of submenu
     */
    maxWidth: {
      type: [String, Number],
      "default": 0
    },

    /**
     * Min width of submenu
     */
    minWidth: {
      type: [String, Number],
      "default": 0
    }
  },
  setup: function setup(props, ctx) {
    var options = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.inject)('globalOptions');

    var _toRefs = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.toRefs)(props),
        adjustSubMenuPosition = _toRefs.adjustSubMenuPosition,
        maxWidth = _toRefs.maxWidth,
        minWidth = _toRefs.minWidth;

    var adjustSubMenuPositionValue = typeof adjustSubMenuPosition.value !== 'undefined' ? adjustSubMenuPosition.value : options.adjustPosition; //Create Item

    return function () {
      return (0,external_commonjs_vue_commonjs2_vue_root_Vue_.h)(ContextMenuItem, _objectSpread2(_objectSpread2({}, props), {}, {
        showRightArrow: true,
        maxWidth: undefined,
        minWidth: undefined,
        adjustSubMenuPosition: undefined,
        hasChildren: _typeof(ctx.slots["default"]) !== undefined
      }), ctx.slots["default"] ? {
        //Create SubMenu
        submenu: function submenu() {
          return (0,external_commonjs_vue_commonjs2_vue_root_Vue_.h)(ContextSubMenu, {
            maxWidth: maxWidth.value,
            minWidth: minWidth.value,
            adjustPosition: adjustSubMenuPositionValue
          }, {
            "default": ctx.slots["default"]
          });
        }
      } : undefined);
    };
  }
}));
;// CONCATENATED MODULE: ./src/ContextMenuGroup.vue?vue&type=script&lang=ts
 
;// CONCATENATED MODULE: ./src/ContextMenuGroup.vue



const ContextMenuGroup_exports_ = ContextMenuGroupvue_type_script_lang_ts;

/* harmony default export */ const ContextMenuGroup = (ContextMenuGroup_exports_);
;// CONCATENATED MODULE: ./src/ContextMenuInstance.ts










function initInstance(options, container, isNew, customSlots) {
  var vnode = (0,external_commonjs_vue_commonjs2_vue_root_Vue_.h)(ContextSubMenuWrapper, {
    options: options,
    show: true,
    container: container,
    isFullScreenContainer: !isNew,
    onClose: function onClose() {
      return (0,external_commonjs_vue_commonjs2_vue_root_Vue_.render)(null, container);
    }
  }, customSlots);
  (0,external_commonjs_vue_commonjs2_vue_root_Vue_.render)(vnode, container);
  return vnode.component;
} //Show global contextmenu


function $contextmenu(options, customSlots) {
  var container = genContainer(options);
  var component = initInstance(options, container.container, container.isNew, customSlots);
  return component.exposed;
}

/* harmony default export */ const ContextMenuInstance = ({
  /**
   * For Vue install
   * @param app
   */
  install: function install(app) {
    app.config.globalProperties.$contextmenu = $contextmenu;
    app.component('ContextMenu', ContextMenu);
    app.component('ContextMenuItem', ContextMenuItem);
    app.component('ContextMenuGroup', ContextMenuGroup);
    app.component('ContextMenuSperator', ContextMenuSeparator);
    app.component('ContextMenuSeparator', ContextMenuSeparator);
    app.component('ContextSubMenu', ContextSubMenu);
  },

  /**
   * Show a ContextMenu in page, same as `this.$contextmenu`
   * @param options The options of ContextMenu
   * @param customSlots You can provide some custom slots to customize the rendering style of the menu. These slots are the same as the slots of component ContextMenu.
   * @returns Menu instance
   */
  showContextMenu: function showContextMenu(options, customSlots) {
    return $contextmenu(options, customSlots);
  },
  //Close the currently open menu
  closeContextMenu: closeContextMenu,
  //Tools
  transformMenuPosition: transformMenuPosition
});
;// CONCATENATED MODULE: ./index.js



;






/* harmony default export */ const index = (ContextMenuInstance);
;// CONCATENATED MODULE: ./node_modules/@vue/cli-service/lib/commands/build/entry-lib.js


/* harmony default export */ const entry_lib = (index);


})();

/******/ 	return __webpack_exports__;
/******/ })()
;
});
//# sourceMappingURL=vue3-context-menu.umd.js.map