package com.hmdp.service.impl;

import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import com.hmdp.utils.RegexUtils;

import javax.servlet.http.HttpSession;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1.校验手机号是否正确
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.错误，返回失败
            return Result.fail("手机号格式错误！");
        }

        // 3.正确，生成验证码
        String code = RandomUtil.randomNumbers(6);

        // 4.保存验证码到session
        session.setAttribute("code" + phone, code);

        // 5.发送验证码
        log.debug("发送短信验证码成功，验证码：{}", code);

        // 6.返回正确
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1.检验手机号是否正确
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.错误，返回失败
            return Result.fail("手机号格式错误！");
        }

        // 3.从session中取出验证码
        String code = (String) session.getAttribute("code" + phone); // 修改，使得手机号和验证码一一对应

        // 4.检验验证码是否正确
        if (code == null || !code.equals(loginForm.getCode())) {
            // 5.错误，返回失败
            return Result.fail("验证码错误！");
        }

        // 6.从数据库中查找手机号是否存在
        User user = query().eq("phone", phone).one();

        // 7.不存在，创建新用户
        if (user == null) {
            user = createWithPhone(phone);
        }

        // 8.保存用户信息到session
        UserDTO userDTO = new UserDTO();
        BeanUtils.copyProperties(user, userDTO);
        session.setAttribute("user", userDTO);

        // 9.返回成功
        return Result.ok();
    }

    private User createWithPhone(String phone) {
        User user = User.builder()
                .phone(phone)
                .nickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10))
                .build();
        save(user);
        return user;
    }
}
