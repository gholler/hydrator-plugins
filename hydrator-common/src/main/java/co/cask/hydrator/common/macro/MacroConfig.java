/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.common.macro;

import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.batch.BatchContext;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A type of plugin config that supports macro substitution.
 * Macros are of the format ${macro-type(macro-arguments)}.
 * For example, ${runtime(1d,yyyy-MM-dd)} is a macro with macro-type 'runtime' and macro-arguments '1d,yyyy-MM-dd'.
 *
 * The '{' and '}' are treated as special characters and cannot be used in the macro-type or macro-arguments.
 *
 * Currently, the only macro-type is 'runtime'. See {@link RuntimeMacro} for details.
 *
 * TODO: add support for other types of substitution.
 * Some ideas could be for ${token[key]:type} to read a specific key from the workflow token
 * If this gets any more complicated, could look into using some grammar and parser
 */
public abstract class MacroConfig extends PluginConfig {

  private static final String[] ESCAPED_TOKENS = {"\\${", "\\{", "\\}", "\\(",
                                                  "\\)"};

  /**
   * Validate that macros can be substituted.
   */
  public void validate() {
    validate(true);
  }

  /**
   * Validate that macros can be substituted and that there is no invalid macro syntax.
   */
  public void validate(boolean isLenient) {
    MacroContext macroContext = new DefaultMacroContext(0);
    substituteMacros(macroContext, isLenient);
  }

  /**
   * Performs macro substitution on all non-static string fields.
   *
   * @param batchRuntimeContext runtime context for batch etl plugins
   * @param fields the fields to perform macro substitution on. If nothing is given, every field will
   *               be substituted.
   * @throws InvalidMacroException if any macro is invalid
   */
  public void substituteMacros(BatchRuntimeContext batchRuntimeContext, String... fields) {
    MacroContext macroContext = new DefaultMacroContext(batchRuntimeContext.getLogicalStartTime(),
                                                        batchRuntimeContext.getRuntimeArguments());
    substituteMacros(macroContext, true, fields);
  }

  /**
   * Performs macro substitution on all non-static string fields.
   *
   * @param batchContext context for batch etl plugins
   * @param fields the fields to perform macro substitution on. If nothing is given, every field will
   *               be substituted.
   * @throws InvalidMacroException if any macro is invalid
   */
  public void substituteMacros(BatchContext batchContext, String... fields) {
    MacroContext macroContext = new DefaultMacroContext(batchContext.getLogicalStartTime(),
                                                        batchContext.getRuntimeArguments());
    substituteMacros(macroContext, true, fields);
  }

  /**
   * Performs macro substitution on all non-static string fields.
   *
   * @param macroContext context for macro substitution
   * @param fields the fields to perform macro substitution on. If nothing is given, every field will
   *               be substituted.
   * @throws InvalidMacroException if any macro is invalid
   */
  public void substituteMacros(MacroContext macroContext, String... fields) {
    substituteMacros(macroContext, true, fields);
  }

  /**
   * Performs macro substitution on all non-static string fields.
   *
   * @param macroContext context for macro substitution
   * @param isLenient whether invalid macro syntax should throw an exception
   * @param fields the fields to perform macro substitution on. If nothing is given, every field will
   *               be substituted.
   * @throws InvalidMacroException if any macro is invalid
   */
  public void substituteMacros(MacroContext macroContext, boolean isLenient, String... fields) {
    Set<String> whitelist;
    if (fields.length == 0) {
      whitelist = null;
    } else {
      whitelist = new HashSet<>();
      Collections.addAll(whitelist, fields);
    }

    for (Field field : getClass().getDeclaredFields()) {
      // skip static fields
      if (Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      if (field.getType() == String.class) {
        if (whitelist != null && !whitelist.contains(field.getName())) {
          continue;
        }
        try {
          field.setAccessible(true);
          String fieldValue = (String) field.get(this);
          if (fieldValue != null) {
            field.set(this, substitute(fieldValue, macroContext, isLenient));
          }
        } catch (IllegalAccessException e) {
          // can't do anything, just ignore
        }
      }
    }
  }

//  /**
//   * Substitute all macros in the specified string. Supports nested macros.
//   *
//   * Not implemented in the most efficient way, as it makes a pass for every macro in the string.
//   * But this keeps the logic simple.
//   *
//   * @param str the string to substitute
//   * @param macroContext context for macros
//   * @param isLenient whether invalid macro syntax should throw an exception
//   * @return the substituted string
//   * @throws InvalidMacroException if any invalid macro syntax was found
//   */
//  private String substitute(String str, MacroContext macroContext, boolean isLenient) {
//    MacroPosition macroPosition = findRightmostMacro(str, str.length(), isLenient);
//    while (macroPosition != null) {
//      try {
//        str = str.substring(0, macroPosition.startIndex) +
//          macroPosition.getMacroValue(macroContext) +
//          // + 1 since the end index is the index of the enclosing '}'
//          str.substring(macroPosition.endIndex + 1);
//      } catch (Exception e) {
//        throw new InvalidMacroException(String.format("Invalid macro '%s'.", macroPosition.macroStr), e);
//      }
//
//      macroPosition = findRightmostMacro(str, str.length(), isLenient);
//    }
//    return str;
//  }

  /**
   * Substitute all macros in the specified string. Supports nested macros.
   *
   * Not implemented in the most efficient way, as it makes a pass for every macro in the string.
   * But this keeps the logic simple.
   *
   * @param str the string to substitute
   * @param macroContext context for macros
   * @param isLenient whether invalid macro syntax should throw an exception
   * @return the substituted string
   * @throws InvalidMacroException if any invalid macro syntax was found
   */
  private String substitute(String str, MacroContext macroContext, boolean isLenient) {
    return replaceEscapedSyntax(substitute(str, macroContext, isLenient, 0));
  }

  private String substitute(String str, MacroContext macroContext, boolean isLenient, int depth) {
    if (depth > 10) {
      throw new InvalidMacroException(String.format("Failed substituting maco '%s', expansion exceeded 10 levels.",
                                                    str));
    }

    MacroPosition macroPosition = findRightmostMacro(str, str.length(), isLenient);
    while (macroPosition != null) {
      try {
        str = str.substring(0, macroPosition.startIndex) +
          substitute(macroPosition.getMacroValue(macroContext), macroContext, isLenient, depth + 1) +
          str.substring(macroPosition.endIndex + 1);
      } catch (Exception e) {
        throw new InvalidMacroException(e.getMessage());
      }

      macroPosition = findRightmostMacro(str, str.length(), isLenient);
    }

    return str;
  }

  /**
   * Find the rightmost macro in the specified string, ignoring all characters after the specified index.
   * If no macro is found, returns null.
   *
   * @param str the string to find a macro in
   * @param fromIndex ignore all characters to the right of this index
   * @param isLenient whether to throw an exception if invalid macro syntax is found
   * @return the rightmost macro and its position in the string
   * @throws InvalidMacroException if invalid macro syntax was found. This cannot be thrown if isLenient is true.
   */
  @Nullable
  private MacroPosition findRightmostMacro(String str, int fromIndex, boolean isLenient) {
    int startIndex = str.lastIndexOf("${", fromIndex);
    // skip all escaped syntax '\${'
    while (startIndex > 0 && str.charAt(startIndex - 1) == '\\') {
      startIndex = str.substring(0, startIndex - 1).lastIndexOf("${", fromIndex);
    }

    if (startIndex < 0) {
      return null;
    }

    // found "${", now look for enclosing "}" and allow escaping through \}
    int endIndex = str.indexOf('}', startIndex);
    while (endIndex > 0 && str.charAt(endIndex - 1) == '\\') {
      endIndex = str.indexOf('}', endIndex + 1);
    }


    // if none is found, there is not a macro
    if (endIndex < 0 || endIndex > fromIndex) {
      throw new InvalidMacroException(String.format("Could not find enclosing '}' for macro '%s'.",
                                                      str.substring(startIndex, fromIndex)));
    }

    // macroStr = 'macroType(macro-arguments)' or just 'property'
    String macroStr = str.substring(startIndex + 2, endIndex).trim();
    String type = macroStr;
    String arguments = null;

    // look for '(', which indicates there are arguments and skip all escaped syntax '\('
    int argsStartIndex = macroStr.indexOf('(');
    while (argsStartIndex > 0 && str.charAt(argsStartIndex - 1) == '\\') {
      argsStartIndex = macroStr.indexOf('(', argsStartIndex);
    }

    if (argsStartIndex > 0) {
      // if there is no enclosing ')'
      boolean lastParenEscaped = macroStr.endsWith(")") && macroStr.charAt(macroStr.lastIndexOf(')') - 1) == '\\';
      if (!macroStr.endsWith(")") || lastParenEscaped) {
        throw new InvalidMacroException(String.format("Could not find enclosing ')' for macro arguments in '%s'.",
                                                        macroStr));
      }
      arguments = macroStr.substring(argsStartIndex + 1, macroStr.length() - 1);
      type = macroStr.substring(0, argsStartIndex);
    } else {
      // the macro is just a property substitution
      type = "property";
      arguments = macroStr;
    }

    type = replaceEscapedSyntax(type);
    arguments = replaceEscapedSyntax(arguments);

    Macro macro = Macros.fromType(type);
    if (macro == null) {
      throw new InvalidMacroException(String.format("Unknown macro type '%s'.", type));
    }
    return new MacroPosition(macroStr, macro, startIndex, endIndex, arguments);
  }

  /**
   * Removes all escaped syntax for all macro syntax symbols: ${, {, } }, (, )
   * @param str the string to replace escaped syntax in
   * @return the string with no escaped syntax
   */
  private String replaceEscapedSyntax(String str) {
    for (String token : ESCAPED_TOKENS) {
      str = str.replace(token, token.substring(1));
    }
    return str;
  }

  private static class MacroPosition {
    private final String macroStr;
    private final Macro macro;
    private final int startIndex;
    private final int endIndex;
    @Nullable
    private final String arguments;

    private MacroPosition(String macroStr, Macro macro, int startIndex, int endIndex, @Nullable String arguments) {
      this.macroStr = macroStr;
      this.macro = macro;
      this.startIndex = startIndex;
      this.endIndex = endIndex;
      this.arguments = arguments;
    }

    private String getMacroValue(MacroContext context) throws Exception {
      return macro.getValue(arguments, context);
    }
  }
}
